---
title: WebAssembly in WeScale
---

# WasmPlugin

- Feature: WasmPlugin
- Start Date: 2024-05-31
- Authors: @earayu

# WebAssembly in WeScale

## What is WebAssembly?

WebAssembly (Wasm) is a rising portable binary format designed for executing code. This code runs almost at native speed within a secure, memory-safe sandbox that has defined resource limits and an API for interaction with the host environment, such as a proxy.

## Benefits

* **Agility**. Extensions can be dynamically delivered and updated at runtime from the control plane. This facilitates the use of a standard, unaltered version of a proxy to load bespoke extensions and allows for bugs to be fixed and updates to be applied directly at runtime without the need to deploy a new binary.

* **Reliability and Isolation**. Deploying extensions within a sandbox that imposes resource limits means that even if an extension crashes or leaks memory, it does not compromise the stability of the entire proxy. Additionally, the allocation of CPU and memory resources can be controlled.

* **Security**. Extensions operate within a sandbox where they communicate with the proxy through a well-defined API, limiting their ability to alter only specific aspects of the connection or request. The proxy also plays a critical role in safeguarding sensitive data by concealing or sanitizing it from the extensions, such as "Authorization" and "Cookie" headers, or the client’s IP address.

* **Diversity**. WebAssembly supports compilation from over 30 programming languages, enabling developers of various backgrounds (including C, Go, Rust, Java, TypeScript, etc.) to create Proxy-Wasm extensions in their preferred language.

* **Maintainability**. Extensions benefit from being developed using standard libraries, separate from the proxy’s own codebase, which ensures a stable ABI (Application Binary Interface).

* **Portability**. The interface that facilitates interaction between the host environment and extensions is designed to be proxy-agnostic. This means that Proxy-Wasm extensions can be used across different proxies like Envoy, NGINX, ATS, or even within the gRPC library, provided they adhere to the standard framework.

## Drawbacks

* **Increased Memory Usage**: Each virtual machine requires its own memory block, leading to higher overall memory consumption.

* **Reduced Performance for Payload Transcoding**: Extensions that transcode the payload experience lower performance due to the necessity of moving substantial amounts of data into and out of the sandbox.

* **Decreased Performance for CPU-Intensive Tasks**: For CPU-bound extensions, performance can be up to twice as slow as native code execution.

* **Larger Binary Sizes**: Including the Wasm runtime increases the binary size significantly, approximately 20MB for WAVM and 10MB for V8.

* **Maturing Ecosystem**: The WebAssembly ecosystem is relatively young, with current development primarily geared towards in-browser applications, where JavaScript is the usual host environment.

## High-level overview

We've implemented a builtin action called `WasmPlugin` that can load a WebAssembly binary and execute it in the context of a request. 
The WebAssembly binary can be used to modify the request, response, and can be used to implement custom logic such as authentication, rate limiting, data masking, etc.

In order to execute provided WebAssembly binary, WeScale needs to embed a Wasm runtime, which will execute the code in a sandbox.

![wasm1.png](images%2Fwasm1.png)

## Abi Specification

The ABI specification defines the interface between the Wasm plugin(`Guest Environment`) and WeScale(`Host Environment`).
WeScale provides a set of functions that the Wasm plugin can call to interact with the host environment. 
WeScale also assumes that the Wasm plugin implements a set of functions that the host environment can call to handle events from the host environment.

Developers can write Wasm plugins using their programming language of choice, but they need to follow the ABI specification 
to ensure that the Wasm plugin can interact with the host environment. Currently, we provide an Go SDK to help developers write Wasm plugins.

The v1 ABI specification is defined in the [wescale-wasm-plugin-sdk](https://github.com/wesql/wescale-wasm-plugin-sdk/tree/main/pkg) repository.

> From the perspective of the Wasm plugin, it can call all the functions on the left side, and it needs to implement all the functions on the right side.

![wasm2.png](images%2Fwasm2.png)


## The WeScale-Wasm-Plugin-Sdk and WeScale-Wasm-Plugin-Template

To facilitate the development of Wasm plugins, we provide the [WeScale-Wasm-Plugin-Sdk](https://github.com/wesql/wescale-wasm-plugin-sdk) 
and [WeScale-Wasm-Plugin-Template](https://github.com/wesql/wescale-wasm-plugin-template/) projects.

The `WeScale-Wasm-Plugin-Sdk` implements the ABI specification, and the `WeScale-Wasm-Plugin-Template` provides a template for developers to write their Wasm plugins.

Most of the time, developers only need to focus on the business logic of the Wasm plugin.