---
标题: WeScale 中的 WebAssembly
---

# WasmPlugin

- 功能名称: WasmPlugin
- 开始日期: 2024-05-31
- 作者: @earayu

# WeScale 中的 WebAssembly

## 什么是 WebAssembly？

WebAssembly (Wasm) 是一种新兴的可移植、可执行的二进制格式。该代码在一个安全的沙盒中运行，可自定义资源限制，可和与宿主环境（如代理）交互的 API，并且几乎可以达到原生的运行速度。

## 优势

* **敏捷性**。可以从控制平面动态地部署和更新所要运行的Wasm代码，这有助于使用标准的、未更改的代理版本加载定制的Wasm扩展，而无需部署新的控制平面二进制文件。

* **可靠性与隔离性**。在进行了资源限制的沙盒中部署Wasm扩展意味着即使Wasm扩展崩溃或内存泄漏，也不会影响整个代理的稳定性。此外，也可以控制对Wasm扩展的 CPU 和内存资源的分配。

* **安全性**。扩展在沙盒中运行，通过定义良好的 API 与代理进行通信，限制其仅能修改连接或请求的特定方面。通过隐藏或清理扩展无法访问的敏感信息，例如 "Authorization" 、 "Cookie" 头、客户端的 IP 地址，代理在保护敏感数据方面发挥关键作用。

* **多样性**。WebAssembly 支持从超过 30 种编程语言进行编译，使得不同背景的开发人员（包括 C、Go、Rust、Java、TypeScript 等）可以用他们喜欢的语言创建 Proxy-Wasm 扩展。

* **可维护性**。扩展使用标准库开发，与代理的代码库分离，确保了稳定的 ABI（应用二进制接口）。

* **可移植性**。宿主环境与扩展之间交互的接口设计可以与具体的代理无关。这意味着 Proxy-Wasm 扩展可以跨不同的代理使用，如 Envoy、NGINX、ATS，甚至在 gRPC 库中使用，只要它们符合标准框架。

## 缺点

* **增加内存使用**: 每个虚拟机都需要自己的内存空间，导致整体内存消耗增加。

* **降低性能**: 数据需要在沙盒和宿主机之间进行交互，降低了系统的性能。

* **CPU 密集型的任务性能下降**: 对于 CPU 密集型扩展，性能可能比原生代码执行慢一倍。

* **较大的二进制文件**: 包含 Wasm 运行时的二进制文件的大小较大，WAVM 大约为 20MB， V8 大约为 10MB。

* **不成熟的生态系统**: WebAssembly 的生态系统相对较新，目前的开发主要面向浏览器内应用程序，宿主环境通常是JavaScript。

## 概述

我们实现了一个名为 `WasmPlugin` 的内置操作，可以加载 WebAssembly 二进制文件并在SQL执行的上下文中执行它。WebAssembly 二进制文件可用于修改请求、响应，还可以用于实现自定义逻辑，如身份验证、限流、数据屏蔽等。

为了执行提供的 WebAssembly 二进制文件，WeScale 需要嵌入一个 Wasm 运行时，该运行时将在沙盒中执行代码。

![wasm1.png](images%2Fwasm1.png)

## ABI 规范

ABI 规范定义了 Wasm 插件（`Guest Environment`）和 WeScale（`Host Environment`）之间的接口。
WeScale 提供了一组函数，Wasm 插件可以调用这些函数与宿主环境进行交互。
WeScale 还假设 Wasm 插件实现了一组函数，宿主环境可以调用这些函数来处理来自宿主环境的事件。

开发人员可以使用他们选择的编程语言编写 Wasm 插件，但他们需要遵循 ABI 规范，以确保 Wasm 插件能够与宿主环境交互。目前，我们提供了一个 Go SDK 来帮助开发人员编写 Wasm 插件。

v1 ABI 规范定义在 [wescale-wasm-plugin-sdk](https://github.com/wesql/wescale-wasm-plugin-sdk/tree/main/pkg) 仓库中。

> 从 Wasm 插件的角度来看，它可以调用左侧的所有函数，并且需要实现右侧的所有函数。

![wasm2.png](images%2Fwasm2.png)

## WeScale-Wasm-Plugin-Sdk 和 WeScale-Wasm-Plugin-Template

为了方便 Wasm 插件的开发，我们提供了 [WeScale-Wasm-Plugin-Sdk](https://github.com/wesql/wescale-wasm-plugin-sdk) 和 [WeScale-Wasm-Plugin-Template](https://github.com/wesql/wescale-wasm-plugin-template/) 项目。

`WeScale-Wasm-Plugin-Sdk` 实现了 ABI 规范，`WeScale-Wasm-Plugin-Template` 为开发人员编写 Wasm 插件提供了模板。

大多数时候，开发人员只需专注于 Wasm 插件的业务逻辑。