# pubsub-broker

[![Build and Test](https://github.com/Hoot-Code/pubsub-broker/actions/workflows/ci.yml/badge.svg)](https://github.com/Hoot-Code/pubsub-broker/actions/workflows/ci.yml) [![GitHub](https://img.shields.io/badge/GitHub-Hoot--Code/pubsub--broker-blue)](https://github.com/Hoot-Code/pubsub-broker) | [English](README.md)

یک message broker آماده برای محیط تولید، مستقل و بدون هیچ وابستگی خارجی، نوشته‌شده با Go خالص. این پروژه توپیک‌های پارتیشن‌بندی‌شده، تحویل دقیقاً-یک‌بار، خوشه‌بندی چندگره‌ای داخلی، معیارهای سازگار با Prometheus و ردیابی سازگار با OpenTelemetry را ارائه می‌دهد — همه در قالب یک باینری استاتیک که می‌توان بدون نیاز به runtime در هر محیطی اجرا کرد.

## ویژگی‌ها

- **توپیک‌های پارتیشن‌بندی‌شده** — توزیع قطعی پیام‌ها از طریق هش FNV-1a روی کلید، یا round-robin در تعداد پارتیشن قابل تنظیم
- **گروه‌های مصرف‌کننده** — چند مصرف‌کننده مستقل آفست‌های commit‌شده را به ازای هر پارتیشن دنبال می‌کنند؛ پشتیبانی از seek-to-offset و seek-to-timestamp
- **تحویل push** — فریم‌های `CmdPush` آغازشده از سمت سرور، polling را حذف می‌کنند؛ مشتریان یک بار subscribe می‌کنند و پیام‌ها را به‌محض ورود دریافت می‌کنند
- **دوام مبتنی بر WAL** — هر انتشار پیش از نوشتن روی segment، در Write-Ahead Log ذخیره می‌شود؛ broker بدون از دست دادن پیام از خرابی سیستم‌عامل بازیابی می‌شود
- **همانندسازی ISR** — ردیابی In-Sync Replica با نوشتن quorum؛ followerهایی که عقب می‌مانند به طور خودکار از مجموعه ISR حذف می‌شوند
- **انتخاب رهبر Bully** — انتخاب رهبر تک‌مرحله‌ای با تشخیص خرابی مبتنی بر heartbeat (برای محدودیت‌ها به ARCHITECTURE.md مراجعه کنید)
- **TLS** — TLS 1.3 روی هر دو پورت پروتکل باینری و پورت HTTP مدیریتی
- **فشرده‌سازی پیام** — کدک flate/zlib به ازای هر پیام در زمان انتشار مذاکره می‌شود و برای مصرف‌کنندگان شفاف است
- **Seek-to-timestamp** — اسکن جستجوی دودویی اولین آفستی را پیدا می‌کند که timestamp رکورد آن ≥ مقدار نانوثانیه داده‌شده باشد
- **خاموشی کنترل‌شده** — `Stop()` تا پایان تمام درخواست‌های در حال پردازش منتظر می‌ماند، سپس اتصالات را قطع می‌کند

## مقایسه

| ویژگی                  | pubsub-broker    | NSQ        | NATS core          |
|------------------------|------------------|------------|--------------------|
| دوام                   | ✅ WAL + segment  | ✅ دیسک    | ❌ فقط حافظه       |
| پارتیشن                | ✅               | ❌         | ❌ (فقط JetStream) |
| گروه مصرف‌کننده        | ✅               | ✅ channel | ❌                 |
| خوشه‌بندی              | ✅ Bully + ISR    | ✅         | ✅                 |
| تحویل دقیقاً-یک‌بار    | ✅ SeqNum dedup  | ❌         | ❌                 |
| تحویل push             | ✅               | ✅         | ✅                 |
| فشرده‌سازی             | ✅ flate / zlib  | ✅         | ✅                 |
| باینری بدون وابستگی    | ✅               | ✅         | ✅                 |

## راه‌اندازی سریع (Docker)

> **نکته درباره پارتیشن‌ها:** پیام‌ها بر اساس هش کلید (یا round-robin در صورت نبود کلید) به پارتیشن‌ها هدایت می‌شوند. پیامی با کلید `"order-1"` همیشه به همان پارتیشن می‌رسد، اما آن پارتیشن **لزوماً** پارتیشن 0 نیست. از `brokectl tail --topic <t>` (بدون پرچم `--partition`) برای اسکن همه پارتیشن‌ها استفاده کنید — فرض نکنید پیام در پارتیشن 0 قرار دارد.

```bash
docker-compose up -d
brokectl --addr 127.0.0.1:9000 topic create --name orders --partitions 4
brokectl --addr 127.0.0.1:9000 publish --topic orders --key order-1 --payload '{"id":1,"amount":99.00}'
brokectl --addr 127.0.0.1:9000 consumer list
brokectl --addr 127.0.0.1:9000 tail --topic orders --count 5
brokectl --addr 127.0.0.1:9000 health
```

`tail` به صورت پیش‌فرض همه پارتیشن‌ها را اسکن می‌کند، چون پیام‌ها بر اساس هش کلید توزیع می‌شوند — نیازی نیست بدانید پیام در کدام پارتیشن است تا آن را پیدا کنید.

## نصب یک‌کلیکی

برای شروع سریع بدون Docker، اسکریپت quickstart را اجرا کنید:

```bash
curl -fsSL https://raw.githubusercontent.com/Hoot-Code/pubsub-broker/main/quickstart.sh | bash
```

یا اگر مخزن را clone کرده‌اید:

```bash
chmod +x quickstart.sh && ./quickstart.sh
```

این اسکریپت broker و brokectl را build می‌کند، یک توپیک نمونه می‌سازد و ۵ پیام منتشر می‌کند. برای توقف broker کلیدهای Ctrl-C را فشار دهید.

quickstart یک راهنمای تعاملی احراز هویت دارد که به شما امکان می‌دهد:
- **خودکار** — تولید خودکار یک API key امن (توصیه‌شده)
- **دستی** — وارد کردن API key دلخواه (حداقل ۳۲ کاراکتر)
- **غیرفعال** — رد کردن احراز هویت (فقط برای توسعه)

## راه‌اندازی سریع (Go SDK)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/Hoot-Code/pubsub-broker/pkg/client"
)

func main() {
    // اتصال به broker — نیازی به وابستگی خارجی نیست.
    c, err := client.Dial("127.0.0.1:9000",
        client.WithDialTimeout(10*time.Second),
        client.WithReadTimeout(30*time.Second),
    )
    if err != nil {
        log.Fatalf("dial: %v", err)
    }
    defer c.Close()

    // احراز هویت (در صورت غیرفعال بودن auth در تنظیمات broker حذف شود).
    if err := c.Authenticate("my-api-key"); err != nil {
        log.Fatalf("auth: %v", err)
    }

    ctx := context.Background()

    // انتشار یک پیام و دریافت آفست تخصیص‌یافته.
    prod := c.NewProducer("orders")
    offset, err := prod.Publish(ctx, "key-1", []byte(`{"amount":99}`), nil)
    if err != nil {
        log.Fatalf("publish: %v", err)
    }
    fmt.Printf("published at offset %d\n", offset)

    // ایجاد یک مصرف‌کننده در گروه نام‌گذاری‌شده و subscribe برای تحویل push.
    // پیام‌ها بر اساس هش کلید در پارتیشن‌ها توزیع می‌شوند، بنابراین
    // پارتیشن مشخص نمی‌کنیم — گروه مصرف‌کننده از همه پارتیشن‌ها دریافت می‌کند.
    cons := c.NewConsumer("my-group", "orders")
    if err := cons.Subscribe(ctx); err != nil {
        log.Fatalf("subscribe: %v", err)
    }
    for msg := range cons.Messages() {
        fmt.Printf("partition=%d offset=%d payload=%s\n", msg.Partition, msg.Offset, msg.Payload)
        // commit آفست برای پیشروی گروه از این پیام.
        _ = cons.Commit(ctx, msg.Partition, msg.Offset)
    }
}
```

## راه‌اندازی سریع (HTTP Gateway)

برای مرورگرها یا زبان‌هایی که SDK بومی ندارند، gateway اختیاری HTTP/WebSocket را فعال کنید (`"gateway": {"enabled": true, "addr": ":8080"}` در `broker.json`، یا `go run ./cmd/gateway -broker-addr 127.0.0.1:9000 -addr :8080` را به عنوان یک پروسه جداگانه اجرا کنید) و از `curl` ساده استفاده کنید:

```bash
# ایجاد توپیک
curl -s -X POST http://127.0.0.1:8080/v1/topics \
     -d '{"name":"orders","partitions":4}'

# انتشار پیام
curl -s -X POST http://127.0.0.1:8080/v1/topics/orders/messages \
     -d '{"key":"order-1","payload":"hello"}'

# دریافت از یک پارتیشن مشخص (REST API مخصوص پارتیشن است —
# کلید "order-1" به یک پارتیشن خاص هش می‌شود، نه لزوماً 0.
# از brokectl tail --topic orders (بدون پرچم --partition) برای اسکن همه استفاده کنید.)
curl -s 'http://127.0.0.1:8080/v1/topics/orders/partitions/0/messages?offset=0&limit=10'
```

برای subscribe از طریق WebSocket، چون پروژه بدون وابستگی است، کلاینت JS/Python بسته‌ای ارائه نمی‌شود — از هر ابزار سازگار با RFC 6455 استفاده کنید، مثلاً [`websocat`](https://github.com/vi/websocat):

```bash
websocat "ws://127.0.0.1:8080/v1/topics/orders/stream?group=my-group&consumer=c1"
```

...یا این قطعه کد Python مینیمال و بدون وابستگی که فقط از کتابخانه استاندارد `socket`/`hashlib`/`base64` استفاده می‌کند (بدون نیاز به پکیج `websockets`، سازگار با سیاست بدون وابستگی پروژه):

```python
import socket, base64, hashlib, os

key = base64.b64encode(os.urandom(16)).decode()
sock = socket.create_connection(("127.0.0.1", 8080))
sock.send((
    "GET /v1/topics/orders/stream?group=my-group HTTP/1.1\r\n"
    "Host: 127.0.0.1:8080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
    f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
).encode())
print(sock.recv(4096))  # پاسخ 101 Switching Protocols + اولین فریم‌ها اینجا می‌رسند
```

## پیکربندی

با ابزار همراه یک فایل پیکربندی پیش‌فرض تولید کنید:

```bash
go run ./cmd/gen-config > broker.json
```

نمونه خروجی (خلاصه‌شده):

```json
{
  "broker":  { "node_id": "node-a" },
  "network": { "host": "0.0.0.0", "port": 9000, "max_connections": 10000,
               "read_timeout": 30000000000, "write_timeout": 30000000000 },
  "storage": { "data_path": "./data", "wal_path": "./data/wal",
               "segment_max_bytes": 134217728, "sync_policy": "always" },
  "auth":    { "enabled": false }
}
```

برای استقرار در Kubernetes به `deploy/k8s/` مراجعه کنید — شامل StatefulSet، ConfigMap، Service و PodDisruptionBudget است.

## معیارهای عملکرد

اندازه‌گیری‌شده روی Linux، AMD64، Intel Core i7-12700K، Go 1.22. اعداد از `go test -bench=. -benchtime=5s ./tests/benchmarks/`.

| معیار                      | عملیات/ثانیه  | MB/s | تأخیر p99 |
|----------------------------|---------------|------|-----------|
| انتشار (payload یک کیلوبایت)  | 220,000       | 220  | 0.6 ms    |
| انتشار (payload شانزده کیلوبایت) | 50,000    | 800  | 1.2 ms    |
| دریافت (دسته ۱۰۰ پیامی)    | 180,000 msg/s | 180  | 0.8 ms    |
| انتشار ExactlyOnce         | 110,000       | 110  | 0.9 ms    |
| انتشار دسته‌ای (۵۰ پیام)    | 400,000 msg/s | 400  | 1.5 ms    |

برای روش‌شناسی کامل و داده‌های واریانس به `tests/benchmarks/README.md` مراجعه کنید.

## معماری

broker یک سرور TCP باینری، یک لاگ segment پیوست‌فقط، یک Write-Ahead Log، ردیابی آفست گروه مصرف‌کننده، عضویت خوشه اختیاری و یک سرور HTTP مدیریتی را در یک ارکستراتور `Broker` واحد به هم متصل می‌کند. برای نمودار کامل، مرجع دستورات پروتکل و بررسی عمیق موتور ذخیره‌سازی به [ARCHITECTURE.md](ARCHITECTURE.md) مراجعه کنید.

## داشبورد

broker شامل یک مرکز کنترل عملیاتی تعبیه‌شده است که از `GET /dashboard` (یا `GET /` که به آنجا redirect می‌کند) قابل دسترسی است. داشبورد یک اپلیکیشن تک‌صفحه‌ای چندفایلی (ES modules، بدون مرحله build) است که از طریق `go:embed` با تم تاریک و font stack سیستمی جاسازی شده. هیچ منبع خارجی از CDN بارگذاری نمی‌شود.

### بخش‌ها

- **نمای کلی** — تعداد توپیک/پارتیشن، اتصالات فعال، گروه‌های مصرف‌کننده، وضعیت خوشه، نشانگر سلامت، مدت زمان فعالیت
- **توپیک‌ها** — لیست توپیک با تعداد پارتیشن، تعداد پیام، حجم ذخیره‌سازی، سیاست نگهداری و تعداد گروه مصرف‌کننده
- **پارتیشن‌ها** — جزئیات هر پارتیشن (رهبر، replica، ISR، وضعیت WAL، نشانگر under-replicated، اطلاعات segment)
- **گروه‌های مصرف‌کننده** — جفت‌های گروه+توپیک قابل گسترش که اعضا، وضعیت rebalancing، آفست commit‌شده/جاری و lag به ازای هر پارتیشن را نشان می‌دهند
- **اکتشاف زنده** — دنبال‌کردن زنده پیام مبتنی بر WebSocket با فیلترهای topic/partition/key/producer/payload، قابلیت مکث/ادامه، سقف ۵۰۰ پیام در DOM
- **DLQ** — مرورگر صف پیام‌های مرده با قابلیت replay، حذف، export و پاکسازی دسته‌ای برای هر ورودی (اقدامات فقط برای مدیر)
- **خوشه** — کارت‌های گره، تصویرسازی رهبر/پیرو، اطلاعات داخلی Raft (term، commit index، peer match/next index)، جدول وضعیت ISR
- **معیارها** — نمودارهای بازه‌زمانی (5m/15m/1h/24h) برای نرخ انتشار/مصرف، اتصالات، حافظه، CPU، throughput WAL، lag مصرف‌کننده
- **لاگ‌های حسابرسی** — ۱۰۰ رویداد اخیر با جستجو/فیلتر سمت کلاینت بر اساس client، نوع یا توپیک
- **تنظیمات** — نمایش فقط‌خواندنی پیکربندی فعال (پشتیبانی از ویرایش برای نسخه‌های بعدی برنامه‌ریزی شده)

داشبورد وقتی `auth.enabled` برابر true است به احراز هویت نیاز دارد (قابل تنظیم از طریق `network.dashboard_auth_enabled`). کنترل دسترسی مبتنی بر نقش (RBAC) در سمت کلاینت برای بهبود تجربه کاربری اعمال می‌شود؛ تمام امنیت در سمت سرور اجرا می‌شود.

**جریان احراز هویت:**
- کاربران احراز هویت‌نشده صفحه ورود را در `/dashboard` می‌بینند
- کوکی‌های session از نوع `HttpOnly` و `SameSite=Strict` هستند و پس از ۱۲ ساعت منقضی می‌شوند (قابل تنظیم از طریق `network.dashboard_session_ttl`)
- خروج از سیستم، session را در سمت سرور و کوکی را در سمت کلاینت پاک می‌کند
- session‌های منقضی‌شده به طور خودکار به صفحه ورود redirect می‌شوند

![داشبورد](docs/dashboard-screenshot.png)

## مجوز

MIT — فایل `LICENSE` را ببینید.
