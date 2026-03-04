# Counter part

[English | [한국어](./README.ko.md)]

Multi-purpose tools for convenience in accounting operations

## Key Features

- Statement Analysis for a Specific Account Subject

## Tech Stack

**Backend**  

- **Language:** Python 3.13.2
- **Framework:** FastAPI

**Frontend**  

- **Language:** JavaScript (ES6+)
- **Styling:** CSS3 (Custom Properties, Flexbox)
- **Markup:** HTML5
- **Design Philosophy:** Vanilla JS approach for zero-dependency and optimal performance.

## Tech Decisions

**Why Polars?**  
I chose **polars** instead of pandas to reduce memory usage.

**Why Vanilla JS?**  
I used Vanilla JS for performance optimization and simple functionality.

## Project Structure

```text
.
├── src/
│   ├── config/         # Global config (logging)
│   ├── exceptions/     # Custom error definitions and global exception handlers
│   ├── handlers/       # Request handling logic (Route handlers)
│   ├── routes/         # API endpoint definitions (Controllers)
│   ├── services/       # Core business logic and use cases
│   ├── static/         # Frontend static js and css files
│   ├── templates/      # Frontend HTML files
│   └── tests/          # Pytest suites (Side-effect mocking)
└── README.md
```
