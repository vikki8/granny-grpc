// binary codec for the hotel-workload messages.

#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace hotel::codec {

inline void write_u32(std::vector<uint8_t>& buf, uint32_t v) {
    uint8_t bytes[4];
    bytes[0] = (uint8_t)(v & 0xff);
    bytes[1] = (uint8_t)((v >> 8) & 0xff);
    bytes[2] = (uint8_t)((v >> 16) & 0xff);
    bytes[3] = (uint8_t)((v >> 24) & 0xff);
    buf.insert(buf.end(), bytes, bytes + 4);
}

inline void write_double(std::vector<uint8_t>& buf, double v) {
    uint8_t bytes[8];
    std::memcpy(bytes, &v, 8);  // host LE assumed; Faasm runs on x86_64
    buf.insert(buf.end(), bytes, bytes + 8);
}

inline void write_bytes(std::vector<uint8_t>& buf, const void* data, size_t n) {
    auto* p = reinterpret_cast<const uint8_t*>(data);
    buf.insert(buf.end(), p, p + n);
}

inline uint32_t read_u32(const uint8_t* p, size_t off, size_t cap) {
    if (off + 4 > cap) throw std::runtime_error("hotel_codec: u32 short read");
    uint32_t v = 0;
    v |= (uint32_t)p[off];
    v |= (uint32_t)p[off + 1] << 8;
    v |= (uint32_t)p[off + 2] << 16;
    v |= (uint32_t)p[off + 3] << 24;
    return v;
}

inline double read_double(const uint8_t* p, size_t off, size_t cap) {
    if (off + 8 > cap) throw std::runtime_error("hotel_codec: f64 short read");
    double v;
    std::memcpy(&v, p + off, 8);
    return v;
}

inline constexpr std::size_t kMaxPayloadBytes = 64 * 1024;

inline void appendPadding(std::vector<uint8_t>& buf, std::size_t targetTotalBytes) {

    if (targetTotalBytes < buf.size() + 4) return;
    uint32_t padLen = static_cast<uint32_t>(targetTotalBytes - buf.size() - 4);
    write_u32(buf, padLen);
    buf.insert(buf.end(), static_cast<std::size_t>(padLen),
               static_cast<uint8_t>(0));
}

inline void consumeTrailingPadding(const uint8_t* p, std::size_t& off,
                                   std::size_t cap) {
    if (off == cap) return;  // no padding present
    uint32_t padLen = read_u32(p, off, cap);
    off += 4;
    if (off + padLen != cap)
        throw std::runtime_error("hotel_codec: bad trailing padding");
    off += static_cast<std::size_t>(padLen);
}

struct SearchRequest {
    double lat;
    double lon;
    double radiusKm;
};

inline std::vector<uint8_t> encode(const SearchRequest& req) {
    std::vector<uint8_t> buf;
    buf.reserve(24);
    write_double(buf, req.lat);
    write_double(buf, req.lon);
    write_double(buf, req.radiusKm);
    return buf;
}

inline SearchRequest decodeSearchRequest(const std::vector<uint8_t>& buf) {
    if (buf.size() < 24)
        throw std::runtime_error("hotel_codec: SearchRequest wrong size");
    SearchRequest r;
    r.lat = read_double(buf.data(), 0, buf.size());
    r.lon = read_double(buf.data(), 8, buf.size());
    r.radiusKm = read_double(buf.data(), 16, buf.size());
    std::size_t off = 24;
    consumeTrailingPadding(buf.data(), off, buf.size());
    return r;
}

struct SearchResponse {
    std::vector<uint32_t> hotelIds;
};

inline std::vector<uint8_t> encode(const SearchResponse& resp) {
    std::vector<uint8_t> buf;
    buf.reserve(4 + 4 * resp.hotelIds.size());
    write_u32(buf, (uint32_t)resp.hotelIds.size());
    for (auto id : resp.hotelIds) write_u32(buf, id);
    return buf;
}

inline SearchResponse decodeSearchResponse(const std::vector<uint8_t>& buf) {
    SearchResponse r;
    if (buf.size() < 4)
        throw std::runtime_error("hotel_codec: SearchResponse too short");
    uint32_t n = read_u32(buf.data(), 0, buf.size());
    if (buf.size() < 4 + 4u * n)
        throw std::runtime_error("hotel_codec: SearchResponse size mismatch");
    r.hotelIds.reserve(n);
    for (uint32_t i = 0; i < n; ++i)
        r.hotelIds.push_back(read_u32(buf.data(), 4 + 4u * i, buf.size()));
    std::size_t off = 4 + 4u * n;
    consumeTrailingPadding(buf.data(), off, buf.size());
    return r;
}

struct ProfileRequest {
    uint32_t hotelId;
};

inline std::vector<uint8_t> encode(const ProfileRequest& req) {
    std::vector<uint8_t> buf;
    buf.reserve(4);
    write_u32(buf, req.hotelId);
    return buf;
}

inline ProfileRequest decodeProfileRequest(const std::vector<uint8_t>& buf) {
    if (buf.size() < 4)
        throw std::runtime_error("hotel_codec: ProfileRequest wrong size");
    ProfileRequest r{read_u32(buf.data(), 0, buf.size())};
    std::size_t off = 4;
    consumeTrailingPadding(buf.data(), off, buf.size());
    return r;
}

struct ProfileResponse {
    uint32_t hotelId;
    std::string name;
    std::string address;
    double lat;
    double lon;
    double rate;
};

inline std::vector<uint8_t> encode(const ProfileResponse& resp) {
    std::vector<uint8_t> buf;
    buf.reserve(28 + resp.name.size() + resp.address.size());
    write_u32(buf, resp.hotelId);
    write_u32(buf, (uint32_t)resp.name.size());
    write_bytes(buf, resp.name.data(), resp.name.size());
    write_u32(buf, (uint32_t)resp.address.size());
    write_bytes(buf, resp.address.data(), resp.address.size());
    write_double(buf, resp.lat);
    write_double(buf, resp.lon);
    write_double(buf, resp.rate);
    return buf;
}

inline ProfileResponse decodeProfileResponse(const std::vector<uint8_t>& buf) {
    ProfileResponse r;
    size_t off = 0;
    r.hotelId = read_u32(buf.data(), off, buf.size()); off += 4;
    uint32_t nl = read_u32(buf.data(), off, buf.size()); off += 4;
    if (off + nl > buf.size())
        throw std::runtime_error("hotel_codec: ProfileResponse name overflow");
    r.name.assign((const char*)buf.data() + off, nl); off += nl;
    uint32_t al = read_u32(buf.data(), off, buf.size()); off += 4;
    if (off + al > buf.size())
        throw std::runtime_error("hotel_codec: ProfileResponse addr overflow");
    r.address.assign((const char*)buf.data() + off, al); off += al;
    r.lat = read_double(buf.data(), off, buf.size()); off += 8;
    r.lon = read_double(buf.data(), off, buf.size()); off += 8;
    r.rate = read_double(buf.data(), off, buf.size()); off += 8;
    consumeTrailingPadding(buf.data(), off, buf.size());
    return r;
}

// Geo distance (haversine used by search service) 

inline double haversineKm(double lat1, double lon1, double lat2, double lon2) {
    constexpr double R = 6371.0;
    constexpr double deg2rad = 3.14159265358979323846 / 180.0;
    double dlat = (lat2 - lat1) * deg2rad;
    double dlon = (lon2 - lon1) * deg2rad;
    double a = std::sin(dlat * 0.5) * std::sin(dlat * 0.5)
             + std::cos(lat1 * deg2rad) * std::cos(lat2 * deg2rad)
             * std::sin(dlon * 0.5) * std::sin(dlon * 0.5);
    double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
    return R * c;
}

} 
