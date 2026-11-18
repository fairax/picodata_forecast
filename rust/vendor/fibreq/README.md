# 🛰️ Fibreq (FIBer REQuests)

Fibreq is non-blocking HTTP client designed specifically for the Tarantool ecosystem, leveraging the Tarantool's fiber capabilities. 
It aims to provide a robust, feature-rich alternative for making HTTP(S) requests directly from Tarantool instances, 
inspired by the ease of use seen in libraries like `reqwest` but tailored for Tarantool's unique environment.

## ✨Features

- **Non-blocking Nature**: Utilizes Tarantool's fiber system for efficient, non-blocking I/O operations.
- **Connection Pooling**: Enhances performance by reusing connections across multiple requests, minimizing the overhead of establishing new connections.
- **Comprehensive Timeout Controls**: Offers detailed timeout configurations for connection establishment, request sending, and response reading to avoid hanging requests.
- **HTTPS Support**: Secures your data with TLS support for HTTPS requests, ensuring your connections are encrypted and safe.
- **Flexible Request Building**: Simplifies creating complex requests with a convenient builder pattern, supporting various content types, headers, and body content.
- **Error Handling**: Provides clear and actionable errors, making debugging and error resolution straightforward.

## 📦 Installation

Add Fibreq to your `Cargo.toml`:

```toml
[dependencies]
fibreq = "0.1.0"
```

## 🚀 Quick Start

```rust
fn request() -> Result<(), fibreq::error::Error> {
    let client = fibreq::ClientBuilder::new().build();
    let response = client.get("https://google.ru").unwrap().send().unwrap();

    println!("Status: {}", response.status());
    if let Ok(text) = response.text() {
        println!("Body: {}", text);
    }
    Ok(())
}
```