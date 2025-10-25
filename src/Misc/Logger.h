#pragma once
#include <iostream>
#include <fstream>
#include <mutex>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <functional>
#include <cstdint>

namespace Logger {
    enum class Level { Info, Warning, Error };
    inline std::ofstream logFile;
    inline std::mutex logMutex;
    inline std::function<void(const std::string&)> s_Callback;
    constexpr const char* COLOR_RESET = "\x1b[0m";
    constexpr const char* COLOR_LIGHT_BLUE = "\x1b[94m";
    constexpr const char* COLOR_YELLOW = "\x1b[33m";
    constexpr const char* COLOR_RED = "\x1b[31m";
    constexpr const char* COLOR_GREEN = "\x1b[32m";
    constexpr const char* COLOR_GRAY = "\x1b[90m";

    inline void Init() {




        std::ofstream clearLog("run.log", std::ios::out);
        if (!clearLog) {
            std::cerr << "Failed to truncate run.log\n";
            return;
        }
        logFile.open("run.log", std::ios::app);
        if (!logFile) {
            std::cerr << "Failed to open run.log\n";
        }
    }

    inline void SetCallback(const std::function<void(const std::string&)>& cb) {
        s_Callback = cb;
    }

    template<typename... Args>
    std::string vformat(const char* fmt, Args&&... args) {
        int needed = std::snprintf(nullptr, 0, fmt, std::forward<Args>(args)...);
        if (needed <= 0) return {};
        std::string buf(static_cast<size_t>(needed), '\0');
        std::snprintf(&buf[0], buf.size() + 1, fmt, std::forward<Args>(args)...);
        return buf;
    }

    inline void LogInternal(const std::string& msg, Level level) {
        auto now = std::time(nullptr);
        std::tm tmNow;
        localtime_s(&tmNow, &now);

        std::ostringstream line;
        line << "[" << std::put_time(&tmNow, "%Y-%m-%d %H:%M:%S") << "] " << msg << "\n";

        std::lock_guard<std::mutex> lock(logMutex);
        if (logFile.is_open()) {
            logFile << line.str();
            logFile.flush();
        }
        if (s_Callback) {
            s_Callback(line.str());
        }

        const char* prefixColor = COLOR_GREEN;
        const char* prefixText = "+";
        if (level == Level::Warning) {
            prefixColor = COLOR_YELLOW;
            prefixText = "WARNING";
        }
        else if (level == Level::Error) {
            prefixColor = COLOR_RED;
            prefixText = "ERROR";
        }

        std::cout << COLOR_GRAY << "[" << prefixColor << prefixText << COLOR_GRAY << "] "
            << COLOR_LIGHT_BLUE << msg << COLOR_RESET << std::endl;
    }

    inline void LogToFileOnly(const std::string& msg) {
        auto now = std::time(nullptr);
        std::tm tmNow;
        localtime_s(&tmNow, &now);

        std::ostringstream line;
        line << "[" << std::put_time(&tmNow, "%Y-%m-%d %H:%M:%S") << "] " << msg << "\n";

        std::lock_guard<std::mutex> lock(logMutex);
        if (logFile.is_open()) {
            logFile << line.str();
            logFile.flush();
        }
        if (s_Callback) {
            s_Callback(line.str());
        }
    }

    template<typename... Args>
    void LogPrintf(const char* fmt, Args&&... args) {
        std::string msg = vformat(fmt, std::forward<Args>(args)...);
        LogInternal(msg, Level::Info);
    }

    template<typename... Args>
    void LogWarningPrintf(const char* fmt, Args&&... args) {
        std::string msg = vformat(fmt, std::forward<Args>(args)...);
        LogInternal(msg, Level::Warning);
    }

    template<typename... Args>
    void LogErrorPrintf(const char* fmt, Args&&... args) {
        std::string msg = vformat(fmt, std::forward<Args>(args)...);
        LogInternal(msg, Level::Error);
    }

    inline void Log(const std::string& msg) {
        LogInternal(msg, Level::Info);
    }


    // TITLE MESSAGES


    inline void LogTitleMessage(const std::string& title, const std::string& message, Level level = Level::Info) {
        std::ostringstream oss;
        oss << "[ " << title << " ] " << message;
        LogInternal(oss.str(), level);
    }

    template<typename... Args>
    void LogTitleMessageF(const std::string& title, const char* fmt, Args&&... args) {
        std::string formatted = vformat(fmt, std::forward<Args>(args)...);
        std::ostringstream oss;
        oss << "[ " << title << " ] " << formatted;
        LogInternal(oss.str(), Level::Info);
    }
}

#undef LOG_MESSAGE
#undef LOG_WARNING
#undef LOG_ERROR
#undef LOG_FILE
#undef LOG_ADDR
#undef LOG_MESSAGEF
#undef LOG_WARNINGF
#undef LOG_ERRORF

#define LOG_MESSAGE(expr)      do { std::ostringstream oss; oss << expr; Logger::LogInternal(oss.str(), Logger::Level::Info); } while(0)
#define LOG_WARNING(expr)      do { std::ostringstream oss; oss << expr; Logger::LogInternal(oss.str(), Logger::Level::Warning); } while(0)
#define LOG_ERROR(expr)        do { std::ostringstream oss; oss << expr; Logger::LogInternal(oss.str(), Logger::Level::Error); } while(0)
#define LOG_FILE(expr)         do { std::ostringstream oss; oss << expr; Logger::LogToFileOnly(oss.str()); } while(0)
#define LOG_ADDR(x)            do { std::ostringstream oss; oss << "[ADDR] [" << #x << "] [0x" << std::hex << (std::uint64_t)(x) << "]"; Logger::LogInternal(oss.str(), Logger::Level::Info); } while(0)
#define LOG_MESSAGEF(fmt, ...) do { Logger::LogPrintf(fmt, __VA_ARGS__); } while(0)
#define LOG_WARNINGF(fmt, ...) do { Logger::LogWarningPrintf(fmt, __VA_ARGS__); } while(0)
#define LOG_ERRORF(fmt, ...)   do { Logger::LogErrorPrintf(fmt, __VA_ARGS__); } while(0)

#undef LOG_TITLE
#undef LOG_TITLEF
#undef LOG_TITLE_WARNING
#undef LOG_TITLE_ERROR
#undef LOG_TITLE_WARNINGF
#undef LOG_TITLE_ERRORF

#define LOG_TITLE(title, msg)                do { Logger::LogTitleMessage(title, msg, Logger::Level::Info); } while(0)
#define LOG_TITLE_WARNING(title, msg)        do { Logger::LogTitleMessage(title, msg, Logger::Level::Warning); } while(0)
#define LOG_TITLE_ERROR(title, msg)          do { Logger::LogTitleMessage(title, msg, Logger::Level::Error); } while(0)

#define LOG_TITLEF(title, fmt, ...)          do { Logger::LogTitleMessageF(title, fmt, __VA_ARGS__); } while(0)
#define LOG_TITLE_WARNINGF(title, fmt, ...)  do { std::string f = Logger::vformat(fmt, __VA_ARGS__); Logger::LogTitleMessage(title, f, Logger::Level::Warning); } while(0)
#define LOG_TITLE_ERRORF(title, fmt, ...)    do { std::string f = Logger::vformat(fmt, __VA_ARGS__); Logger::LogTitleMessage(title, f, Logger::Level::Error); } while(0)
