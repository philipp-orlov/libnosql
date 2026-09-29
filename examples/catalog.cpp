// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// A slightly more real shape: a product catalogue with a secondary index,
// kept consistent by putting both writes in one transaction.
//
//   ./example_catalog [path]

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "nosql/nosql.hpp"

using namespace nosql;

namespace {

struct Product
{
    std::uint64_t id;
    std::uint32_t priceCents;
    std::string name;
    std::string category;
};

std::string encode(const Product& p)
{
    std::string out(sizeof p.priceCents, '\0');
    std::memcpy(out.data(), &p.priceCents, sizeof p.priceCents);
    out += p.name;
    out.push_back('\0');
    out += p.category;
    return out;
}

Product decode(std::uint64_t id, Slice v)
{
    Product p;
    p.id = id;
    std::memcpy(&p.priceCents, v.data(), sizeof p.priceCents);
    const std::string rest = v.subspan(sizeof p.priceCents).string();
    const auto sep = rest.find('\0');
    p.name = rest.substr(0, sep);
    p.category = rest.substr(sep + 1);
    return p;
}

/// Secondary index key: "<category>\0<big-endian price>\0<id>" so that a
/// prefix scan on the category walks products cheapest-first.
std::string indexKey(const Product& p)
{
    std::string k = p.category;
    k.push_back('\0');
    for (int shift = 24; shift >= 0; shift -= 8)
        k.push_back(char(p.priceCents >> shift));
    for (int shift = 56; shift >= 0; shift -= 8)
        k.push_back(char(p.id >> shift));
    return k;
}

void upsert(Txn& t, const Product& p)
{
    auto products = t.db("products", DbFlags::Create | DbFlags::IntegerKey);
    auto byPrice = t.db("by_category_price", DbFlags::Create);

    // Retract the old index entry before writing the new one, or a price change
    // would leave a phantom behind.
    if (auto old = products.get(Slice::ref(p.id)))
        byPrice.erase(indexKey(decode(p.id, *old)));

    products.put(Slice::ref(p.id), encode(p));
    byPrice.put(indexKey(p), Slice::ref(p.id));
}

}  // namespace

int main(int argc, char** argv)
{
    const std::filesystem::path path = argc > 1 ? argv[1] : "catalog.db";
    std::filesystem::remove(path);
    Env store = Env::configure().maxSize(1ull << 30).open(path);

    const std::vector<Product> seed = {
        {1, 1299, "chef's knife", "kitchen"},  {2, 499, "peeler", "kitchen"},
        {3, 8999, "stand mixer", "kitchen"},   {4, 2450, "headphones", "audio"},
        {5, 19900, "studio monitor", "audio"}, {6, 799, "cable", "audio"},
    };

    // One transaction, so the table and its index can never disagree.
    store.write([&](Txn& t) {
        for (const Product& p : seed)
            upsert(t, p);
    });

    // A price change, again atomic across both trees.
    store.write([&](Txn& t) { upsert(t, {3, 7499, "stand mixer", "kitchen"}); });

    store.read([&](Txn& t) {
        auto products = t.db("products");
        for (const char* category : {"audio", "kitchen"}) {
            std::printf("%s, cheapest first:\n", category);
            std::string prefix = category;
            prefix.push_back('\0');
            for (auto [k, v] :
                 t.db("by_category_price").prefix(Slice(prefix.data(), prefix.size()))) {
                (void)k;
                const std::uint64_t id = v.as<std::uint64_t>();
                const Product p = decode(id, products.at(Slice::ref(id)));
                std::printf("  %-16s %8.2f\n", p.name.c_str(), p.priceCents / 100.0);
            }
        }
    });

    // A reader started here would still see the state above even while another
    // thread rewrote everything -- that is what the snapshot buys you.
    store.read([](Txn& t) {
        checkIntegrity(t);
        std::printf("\nintegrity ok; sub-databases:");
        for (const std::string& n : t.listDbs())
            std::printf(" %s", n.c_str());
        std::printf("\n");
    });
    return 0;
}
