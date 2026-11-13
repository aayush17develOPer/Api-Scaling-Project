#include <drogon/drogon.h>

#include <atomic>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "ingest/batch_writer.hpp"

namespace {

std::size_t envSize(const char* name, std::size_t def) {
  const char* v = std::getenv(name);
  if (!v || !*v) return def;
  char* end = nullptr;
  unsigned long long n = std::strtoull(v, &end, 10);
  return (end && *end == '\0' && n > 0) ? static_cast<std::size_t>(n) : def;
}

// Validates JSON without building a DOM: SAX parse straight over the request
// buffer, no allocation, no copy. Rejects empty bodies and trailing garbage,
// so every queued row is a single valid JSON value and "[row,row,...]" is
// always a valid JSON array.
//
// Because rows are batched, one row Postgres refuses would fail all ~2000 rows
// in its batch, so this also rejects what jsonb can't store:
//   - raw NUL bytes (RapidJSON treats NUL as end-of-input, and libpq params are C strings)
//   - invalid UTF-8
//   - the \u0000 escape (jsonb: "unsupported Unicode escape sequence")
bool isValidJson(std::string_view body) {
  if (body.empty()) return false;
  if (std::memchr(body.data(), '\0', body.size()) != nullptr) return false;
  if (body.find("\\u0000") != std::string_view::npos) return false;
  rapidjson::MemoryStream ms(body.data(), body.size());
  rapidjson::Reader reader;
  rapidjson::BaseReaderHandler<> handler;  // accepts everything, stores nothing
  return !reader.Parse<rapidjson::kParseValidateEncodingFlag>(ms, handler).IsError();
}

std::unique_ptr<ingest::BatchWriter> g_writer;

drogon::HttpResponsePtr jsonResponse(drogon::HttpStatusCode code, const char* body) {
  auto resp = drogon::HttpResponse::newHttpResponse();
  resp->setStatusCode(code);
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setBody(body);
  return resp;
}

}  // namespace

int main() {
  // In the config file we have disabled compression and logging.
  // It must also contain the "db_clients" block (see README), pointing at PgBouncer.
  drogon::app().loadConfigFile("../config.json");

  // ---------------------------------------------------------------------
  // Ingestion pipeline: queue -> batcher -> multi-row INSERT via PgBouncer
  // ---------------------------------------------------------------------
  ingest::BatchWriter::Options opt;
  opt.queueCapacity = envSize("INGEST_QUEUE_CAP", 1 << 18);  // must be a power of two
  opt.batchMax = envSize("INGEST_BATCH_MAX", 2000);
  opt.writers = envSize("INGEST_WRITERS", 2);
  opt.maxInflight = envSize("INGEST_MAX_INFLIGHT", 8);
  opt.flushInterval = std::chrono::microseconds(envSize("INGEST_FLUSH_US", 5000));

  // One statement text for every batch => one prepared statement, whatever the
  // batch size. The batch is a single JSON array parameter that Postgres expands
  // into rows server-side.
  static const std::string kInsertSql =
      "INSERT INTO events (payload) SELECT value FROM jsonb_array_elements($1::jsonb)";

  g_writer = std::make_unique<ingest::BatchWriter>(
      opt, [](std::string&& jsonArray, std::size_t rows, ingest::BatchWriter::DoneFn done) {
        auto db = drogon::app().getDbClient();  // "default" client from config.json
        db->execSqlAsync(
            kInsertSql,
            [done](const drogon::orm::Result&) { done(true); },
            [done, rows](const drogon::orm::DrogonDbException& e) {
              LOG_ERROR << "batch insert failed (" << rows << " rows dropped): "
                        << e.base().what();
              done(false);
            },
            std::move(jsonArray));
      });

  // DB clients are only ready once the framework is running.
  drogon::app().registerBeginningAdvice([] { g_writer->start(); });

  // On SIGTERM/SIGINT: stop accepting, drain the queue, wait for DB acks, then quit.
  auto gracefulShutdown = [] {
    static std::atomic<bool> once{false};
    if (once.exchange(true)) return;
    std::thread([] {
      const bool clean = g_writer->stop();
      if (!clean) LOG_WARN << "shutdown timed out with unfinished DB writes";
      drogon::app().quit();
    }).detach();
  };
  drogon::app().setTermSignalHandler(gracefulShutdown);
  drogon::app().setIntSignalHandler(gracefulShutdown);

  // POST /ingest  - validate, enqueue, return 202 immediately (DB write is async)
  drogon::app().registerHandler(
      "/ingest",
      [](const drogon::HttpRequestPtr& req,
         std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
        std::string_view body = req->body();
        if (!isValidJson(body)) {
          return callback(jsonResponse(drogon::k400BadRequest, R"({"error":"body must be valid JSON"})"));
        }
        if (!g_writer->tryPush(std::string(body))) {
          // Queue full (DB can't keep up) or shutting down: shed load, don't buffer forever.
          auto resp = jsonResponse(drogon::k503ServiceUnavailable, R"({"error":"overloaded"})");
          resp->addHeader("Retry-After", "1");
          return callback(resp);
        }
        callback(jsonResponse(drogon::k202Accepted, R"({"status":"queued"})"));
      },
      {drogon::Post});

  // GET /stats - after a load test, written == accepted proves rows really landed.
  drogon::app().registerHandler(
      "/stats",
      [](const drogon::HttpRequestPtr&,
         std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
        auto s = g_writer->stats();
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> w(sb);
        w.StartObject();
        w.Key("accepted");    w.Uint64(s.accepted);
        w.Key("rejected");    w.Uint64(s.rejected);
        w.Key("written");     w.Uint64(s.written);
        w.Key("failed");      w.Uint64(s.failed);
        w.Key("batches");     w.Uint64(s.batches);
        w.Key("queue_depth"); w.Uint64(s.queueDepth);
        w.Key("inflight");    w.Uint64(s.inflight);
        w.EndObject();
        auto res = drogon::HttpResponse::newHttpResponse();
        res->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        res->setBody(std::string(sb.GetString(), sb.GetSize()));
        callback(res);
      },
      {drogon::Get});

  // ---------------------------------------------------------------------
  // Existing benchmark routes (unchanged)
  // ---------------------------------------------------------------------

  // GET /simple route
  drogon::app().registerHandler("/simple",
      [](const drogon::HttpRequestPtr& req,
         std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);

        writer.StartObject();
        writer.Key("message");
        writer.String("hi");
        writer.EndObject();

        // Create a standard http response
        auto res = drogon::HttpResponse::newHttpResponse();
        // Set content type to JSON
        res->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        // Move the string buffer into the response body
        res->setBody(std::string(sb.GetString(), sb.GetSize()));
        callback(res);
      },
      { drogon::Get });

  // PATCH /update-something/{id}/{name} route
  drogon::app().registerHandler("/update-something/{id}/{name}",
      [](const drogon::HttpRequestPtr& req,
         std::function<void(const drogon::HttpResponsePtr&)>&& callback,
         const std::string& id,
         const std::string& name) {
        // Validation
        int idNum = 0;
        try {
          idNum = std::stoi(id);
        }
        catch (...) {
          auto resp = drogon::HttpResponse::newHttpResponse();
          resp->setBody(R"({"error":"id must be a number"})");
          resp->setStatusCode(drogon::k400BadRequest);
          resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
          return callback(resp);
        }

        if (name.length() < 3) {
          auto resp = drogon::HttpResponse::newHttpResponse();
          resp->setBody(R"({"error":"name is required and must be at least 3 characters"})");
          resp->setStatusCode(drogon::k400BadRequest);
          resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
          return callback(resp);
        }

        // Query parameters
        auto queryParams = req->getParameters();
        const std::string& v1 = queryParams["value1"];
        const std::string& v2 = queryParams["value2"];

        std::string totalFoo = "";
        std::string_view rawBody = req->body();
        rapidjson::Document doc;

        static const std::vector<std::string> fooKeys = [] {
          std::vector<std::string> keys;
          for (int i = 1; i <= 10; ++i) keys.push_back("foo" + std::to_string(i));
          return keys;
        }();

        // Parse the raw string directly with RapidJSON
        if (!doc.Parse(rawBody.data(), rawBody.size()).HasParseError() && doc.IsObject()) {
          for (const auto& key : fooKeys) {
            if (doc.HasMember(key.c_str())) {
              const auto& val = doc[key.c_str()];
              if (val.IsString()) {
                totalFoo += val.GetString();
                totalFoo += ". ";
              }
              else if (!val.IsNull()) {
                totalFoo += "NON_STRING_VAL";
              }
            }
          }
        }

        for (auto& c : totalFoo) c = toupper(static_cast<unsigned char>(c));

        // Serialization with RapidJSON
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);

        writer.StartObject();
        writer.Key("id");
        writer.String(id.c_str());
        writer.Key("name");
        writer.String(name.c_str());
        writer.Key("value1");
        writer.String(v1.c_str());
        writer.Key("value2");
        writer.String(v2.c_str());
        writer.Key("total_foo");
        writer.String(totalFoo.c_str());

        writer.Key("history");
        writer.StartArray();

        std::string timestamp = trantor::Date::date().toFormattedString(false);
        std::string action = "Action performed by " + name;
        std::string metadata = "This is a string intended to take up space to simulate a medium-sized production API response object.";
        metadata += metadata;

        for (int i = 0; i < 100; ++i) {
          writer.StartObject();
          writer.Key("event_id");
          writer.Int(idNum + i);
          writer.Key("timestamp");
          writer.String(timestamp.c_str());
          writer.Key("action");
          writer.String(action.c_str());
          writer.Key("metadata");
          writer.String(metadata.c_str());
          writer.Key("status");
          writer.String((i % 2 == 0) ? "success" : "pending");
          writer.EndObject();
        }
        writer.EndArray();
        writer.EndObject();

        // Final response
        auto res = drogon::HttpResponse::newHttpResponse();
        res->setBody(std::string(sb.GetString(), sb.GetSize()));
        res->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(res);
      },
      { drogon::Patch });

  // Start the server
  drogon::app().addListener("0.0.0.0", 5555);

  // Automatic thread scaling
  unsigned int cores = std::thread::hardware_concurrency();
  if (cores == 0) cores = 1;
  drogon::app().setThreadNum(cores);

  LOG_INFO << "Server starting with " << cores << " threads";
  LOG_INFO << "Ingest: batchMax=" << opt.batchMax << " writers=" << opt.writers
           << " queueCap=" << opt.queueCapacity;
  LOG_INFO << "Server running on http://127.0.0.1:5555";
  drogon::app().run();
  return 0;
}