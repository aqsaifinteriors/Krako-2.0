<p align="center">
  <img src="assets/krako-logo-main.png" width="240">
</p>

# Krako 2.0

> Energy-efficient, triadic multi-tier inference infrastructure enabling adaptive routing across heterogeneous edge-cloud nodes.

![Build](https://img.shields.io/badge/build-dev-orange)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
![Status](https://img.shields.io/badge/status-active--development-green)
![Architecture](https://img.shields.io/badge/architecture-triadic-purple)

---

## Overview

Krako 2.0 is a hybrid AI infrastructure project for running inference workloads across edge and cloud environments with adaptive routing, energy-aware execution, and heterogeneous hardware support.

Core focus areas:

- CHUNK-based orchestration
- Triadic adaptive inference (SM + SMem + LLM fallback)
- Heterogeneous runtime support (CPU, GPU, NPU, Edge)
- Hybrid backbone + community execution
- Real-time telemetry and energy-aware routing

---

## Installation

Krako 2.0 is currently in active development. To install the project locally:

1. Clone the repository:

```bash
git clone git@github.com:Krako-Labs/Krako-2.0.git
```

2. Enter the project directory:

```bash
cd Krako-2.0
```

3. Pull the latest changes before starting work:

```bash
git pull origin main
```

As implementation modules are added, component-specific setup instructions should be documented in their respective directories.

---

## Contributing

Contributions are welcome. To contribute:

1. Fork the repository and create a feature branch:

```bash
git checkout -b feature/your-change
```

2. Make focused changes and commit with clear messages.
3. Push your branch to your fork.
4. Open a pull request against `main` with:
   - A short summary of the change
   - Why the change is needed
   - Any testing notes or validation steps

Please keep pull requests small and scoped so they can be reviewed quickly.
