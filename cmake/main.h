#pragma once

#include <log-custom.h>
#include <msg.h>
#include <util.h>
#include <version.h>

#include <QGlobalStatic>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonValue>
#include <QObject>
#include <QString>

#include <cassert>
#include <iostream>
#include <log4cxx/logger.h>
#include <memory.h>
#include <utility>

#define LOGGER(name, category) log4cxx::LoggerPtr name(log4cxx::Logger::getLogger(category));

#define NOT_COPYABLE(TypeName)                                                                                         \
  TypeName(TypeName const&) = delete;                                                                                  \
  TypeName& operator=(TypeName const&) = delete;

#define NOT_MOVEABLE(TypeName)                                                                                         \
  TypeName(TypeName&&) = delete;                                                                                       \
  TypeName& operator=(TypeName&&) = delete;

#define NO_COPYMOVE(TypeName)                                                                                          \
  NOT_COPYABLE(TypeName)                                                                                               \
  NOT_MOVEABLE(TypeName)

namespace gadisd {

using HttpStatus = QHttpServerResponse::StatusCode;
using HttpMethod = QHttpServerRequest::Method;

// categorie di log
extern const char* CTX_MAIN;
extern const char* CTX_SERVER;
extern const char* CTX_REST;
extern const char* CTX_DB;
extern const char* CTX_AUTH;
}
