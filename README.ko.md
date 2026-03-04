# 카운터 파트

[[English](./README.md) | 한국어]

회계 감사의 편의를 위한 간단한 다목적 도구

## 주요 기능

- 특정 계정 과목에 대한 전표들의 대변 및 차변 분석

## 사용된 기술

**백엔드**  

- **언어:** Python 3.13.2
- **프레임워크:** FastAPI

**프론트엔드**  

- **언어:** JavaScript (ES6+)
- **스타일:** CSS3 (Custom Properties, Flexbox)
- **문서:** HTML5
- **디자인 철학:** 바닐라 JavaScript를 이용, 의존성 최소화와 필요한 최소한의 성능만 요구하도록 설계

## 기술 결정

**Polars 사용 이유**  
메모리 사용량 감소를 위해 pandas 대신 **polars**을 사용

**바닐라 JavaScript 사용 이유**  
간단한 기능과 성능을 위해서 바닐라 JavaScript를 사용

## Project Structure

```text
.
├── src/
│   ├── config/         # 전역 설정 (로깅)
│   ├── exceptions/     # 커스텀 에러 계층 및 프로그램 전역 에러 핸들러
│   ├── handlers/       # 요청 로직 핸들러
│   ├── routes/         # API 엔드포인트 컨트롤러
│   ├── services/       # 주요 비즈니스 로직
│   └── tests/          # Pytest 파일들
├── static/         # 프론트엔드 정적 파일들 (html, css, js)
└── README.md
```
