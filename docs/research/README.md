# DishtaYantra Research Paper

## Publication Information

**Title:** DishtaYantra: A High-Performance Multi-Language Dataflow Framework with Zero-Copy Inter-Process Communication

**Author:** Ashutosh Sinha (ajsinha@gmail.com)

**Year:** 2025

**Status:** Patent Pending (LMDB Zero-Copy Data Exchange)

**Repository:** https://github.com/ajsinha/dishtayantra

---

## Files Included

| File | Description |
|------|-------------|
| `dishtayantra_paper.pdf` | Compiled PDF ready for arXiv submission |
| `dishtayantra_paper.tex` | LaTeX source file |
| `dishtayantra_paper.md` | Markdown version for web publishing |
| `references/` | Bibliography and reference materials |

---

## arXiv Submission Guidelines

### Category Recommendation
- **Primary:** cs.DC (Distributed, Parallel, and Cluster Computing)
- **Secondary:** cs.SE (Software Engineering), cs.PF (Performance)

### Compilation Instructions

```bash
# Single compilation
pdflatex dishtayantra_paper.tex

# Full compilation with references
pdflatex dishtayantra_paper.tex
pdflatex dishtayantra_paper.tex
```

### Required LaTeX Packages
- amsmath, amssymb, amsfonts
- graphicx, xcolor
- hyperref
- algorithm, algorithmic
- booktabs, multirow
- listings
- tikz, pgfplots
- geometry, fancyhdr
- cite, url, float

---

## Abstract

Real-time data processing systems face fundamental challenges in achieving low-latency computation while supporting heterogeneous computational workloads across multiple programming languages. We present DishtaYantra, a novel high-performance Directed Acyclic Graph (DAG) compute framework that addresses these challenges through three key innovations:

1. **LMDB Zero-Copy Data Exchange** (Patent Pending): 100-1000× performance improvement over JSON serialization
2. **Multi-Language Calculator Framework**: Python, Java (Py4J), C++ (pybind11), Rust (PyO3), REST
3. **GIL-Free Worker Pool**: True CPU parallelism with DAG affinity scheduling

---

## Key Results

| Metric | Value |
|--------|-------|
| LMDB Latency (100KB) | 50 μs |
| End-to-End Latency (P99, 100KB) | 480 μs |
| Single Worker Throughput | 180,000 msg/s |
| 12-Worker Throughput | 1,950,000 msg/s |
| Parallel Efficiency | 90% @ 12 workers |

**Test Hardware:** AMD Ryzen 9 5900X, 64GB RAM, Ubuntu 24.04 LTS

---

## Key Architectural Features

- **Graph/Subgraph Abstractions:** Modular, reusable pipeline components
- **Node Types:** Subscription, Publication, Calculation, Transform, Metronome, Subgraph
- **Edge Abstraction:** Data routing with buffering and filtering
- **Calculator Interface:** Unified API for Python, Java, C++, Rust
- **Pub/Sub Framework:** 10+ backend support (Kafka, Redis, RabbitMQ, LMDB, etc.)
- **Backpressure Management:** Queue-based flow control with multiple strategies
- **Crash Recovery:** Internal cache with checkpoint/replay mechanism

---

## Citation

```bibtex
@article{sinha2025dishtayantra,
  title={DishtaYantra: A High-Performance Multi-Language Dataflow Framework 
         with Zero-Copy Inter-Process Communication},
  author={Sinha, Ashutosh},
  journal={arXiv preprint},
  year={2025},
  note={Patent Pending}
}
```

---

## License

© 2025 Ashutosh Sinha. All rights reserved.

The LMDB Zero-Copy Data Exchange mechanism described in this paper is patent pending.

---

## Contact

- **Email:** ajsinha@gmail.com
- **GitHub:** https://github.com/ajsinha/dishtayantra
