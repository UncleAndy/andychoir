# Architectural Description: Descriptive GUI based on an Event Bus using WebAssembly (WASI Preview 2)

> **Language:** English · [Русский](GUI-concept.ru.md)

### 1. Conceptual Overview (High-Level Idea)
The system is a native Rust host application whose logic and interface are
extended by isolated plugins compiled for the `wasm32-wasip2` target (Wasm
Component Model).

The entire architecture is built on the principles of **EDA (Event-Driven
Architecture)**. Plugins have no direct access to the graphics context, the
operating system, or the GPU. Instead, the host provides a declarative API for
describing the interface, and all interaction between the host's GUI engine and
the WASM plugins is fully encapsulated inside the **System Event Bus**.

---

### 2. Key System Components

1. **Native Host (Rust Engine):** The core application managing the plugin
   lifecycle through the `wasmtime` runtime with Component Model support
   (`wasm_component_model(true)`).
2. **System Event Bus:** The central hub for routing messages (e.g., based on
   `tokio::sync::broadcast` channels, `crossbeam`, or a custom Event Bus).
   Provides asynchrony and loose coupling between components.
3. **GUI Module (Host):** A native graphics engine (recommended `egui` for
   dynamic rendering simplicity, or `Slint` / `Dioxus` for declarativeness).
   It is a **producer** of input events and a **consumer** of drawing commands.
4. **WASM plugins (`wasm32-wasip2`):** Isolated components that subscribe to
   user input events, process them, and send back commands to change the UI
   state.

---

### 3. Interface Specification (WIT - Wasm Interface Type)

Interaction is strictly typed at the Component Model contract level. Example
interface definition (`interface.wit`):

```wit
package my-app:plugin;

interface gui-types {
    enum input-action {
        click,
        text-changed,
        hover
    }

    // Event from GUI to plugin
    record gui-event {
        window-id: u32,
        element-id: u32,
        action: input-action,
        value: string,
    }

    // Command from plugin to GUI
    record gui-command {
        window-id: u32,
        element-id: u32,
        new-text: string,
    }
}

world plugin-world {
    use gui-types.{gui-event, gui-command};

    // Plugin calls this function to send a GUI change command to the host bus
    import send-gui-command: func(cmd: gui-command);

    // Host calls this function to deliver a bus event into the plugin
    export on-system-event: func(event: gui-event);
}
```

---

### 4. Lifecycle and Data Flow

#### Stage A: Initialization and UI construction
1. The host loads the `.wasm` component.
2. On startup, the plugin generates a declarative description of its interface
   (widget tree with IDs) and sends it to the host through an imported function
   (or a startup event).
3. The host (GUI module) stores this element tree locally in memory.

#### Stage B: Input handling (Event flow)
1. **The user interacts with the interface** (e.g., clicks a button with
   `element_id: 10` in window `window_id: 1`).
2. **The host GUI module** intercepts the action, forms an `Event::GuiInput`
   structure, and publishes it to the **Event Bus**.
3. **The Event Bus** asynchronously forwards the message to the plugin
   dispatcher.
4. **The dispatcher** calls the plugin's exported function `on-system-event(event)`.
5. **The plugin** processes the business logic inside its isolated memory
   (e.g., increments a counter).
6. To update the screen, the plugin calls the imported function
   `send-gui-command(cmd)` (e.g., "change the text on the label with `id: 11`").
7. This command enters the **Event Bus** as `Event::GuiCommand`.
8. **The host GUI module**, subscribed to this event type, receives it,
   immediately updates the widget's local state, and redraws the screen on the
   next frame.

---

### 5. Benefits for Agent Implementation
* **Target isolation:** The `wasm32-unknown-unknown` target is completely
  excluded. We rely exclusively on `wasm32-wasip2`. No manual memory
  management, allocators (`alloc`/`free`), or unsafe raw byte-pointer passing.
  `wasmtime` performs type marshalling automatically based on WIT.
* **Loose coupling:** The native host's GUI module is isolated from the WASM
  runtime. It interacts only with its own data structures (`HashMap`) and Rust
  system channels. This allows the GUI to be developed and tested separately
  from the plugin system.
* **Asynchrony (thread safety):** Long computations or network requests inside
  plugins do not block the host's render thread. The host maintains high FPS
  because message exchange happens through non-blocking queues.

---

### Implementation Instructions (Action plan for an agent):
1. Set up a basic Rust host, wire up `wasmtime` and `wasmtime-wasi`.
2. Create the `interface.wit` file describing the input event and interface
   command structures.
3. Generate bindings using `wit-bindgen`.
4. Implement an asynchronous event bus based on channels (e.g.,
   `tokio::sync::broadcast` or `crossbeam`).
5. Integrate a GUI framework (e.g., `egui`) on the host and set up the graphics
   thread's subscription to UI-change events from the plugin.
6. Write a demo WASM plugin implementing a reactive counter (button + text
   field).
