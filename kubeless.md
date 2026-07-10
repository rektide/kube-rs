# kubeless — kube-rs without Kubernetes

This fork makes the kube-rs controller runtime usable as a **general-purpose
level-triggered reconciliation engine** — for any source that can list its
world and stream changes to it — while remaining a fully functional kube-rs
for Kubernetes users. All flags default **on**; a
`default-features = false` build of `kube-runtime` compiles with **zero**
Kubernetes dependencies: no `kube-client`, no `k8s-openapi`, no hyper, no
rustls, no tower.

Base: kube-rs v4.0.0. Everything here is additive gating plus two new seams;
upstream code paths are byte-identical when default features are enabled
(all 78 upstream `kube-runtime` unit tests and 85 `kube-core` tests pass
unchanged).

## The idea

`kube-runtime`'s machinery was always transport-agnostic in *design* —
the Scheduler (debounced, deduplicated delay queue), the Runner (bounded
concurrency, never two futures per key), the reflector Store (atomic
snapshot-swap cache), `applier` (the full requeue loop), and the
`Action`/error-policy vocabulary have no conceptual dependency on
Kubernetes. What welds the crate to Kubernetes is a set of *encodings*:
`ObjectMeta` from generated bindings, the `Api` transport, and the watch
protocol's wire details. This fork cuts at exactly those three points:

```text
┌───────────────────────────────────────────────────────────────────────┐
│  always available (lean)                                              │
│  Scheduler · Runner · FutureHashMap · reflector Store/Writer ·        │
│  watcher::Event init protocol · applier · Action · predicates ·       │
│  Controller (stream constructors + run) · source::WatchSource driver  │
├───────────────────────────────────────────────────────────────────────┤
│  feature "client"       watcher()/watch_object(), Controller::new/    │
│                         owns/watches, the K8s watch-protocol FSM      │
│  feature "events"       events::Recorder (core/v1 Events)             │
│  feature "finalizer"    finalizer() JSON-patch helper                 │
│  feature "wait"         await_condition + conditions                  │
├───────────────────────────────────────────────────────────────────────┤
│  kube-core "k8s-openapi"  real generated types ⇄ kube_core::k8s shims │
└───────────────────────────────────────────────────────────────────────┘
```

## Layer 1: the feature gates and the `kube_core::k8s` seam

### kube-core

`kube-core` gains a default-on **`k8s-openapi`** feature. Every internal use
of a generated Kubernetes type now goes through one seam module,
**`kube_core::k8s`**:

- feature **on**: pure re-exports of the real `k8s-openapi` 0.28 types.
- feature **off**: ~200 lines of structurally identical stand-ins —
  `ObjectMeta` (full field set), `ListMeta`, `OwnerReference`,
  `ManagedFieldsEntry`, `FieldsV1`, `Time` (the same newtype over
  `jiff::Timestamp` that k8s-openapi 0.28 uses), `ObjectReference`,
  `LabelSelector`, and the `ResourceScope` marker traits. Field names,
  optionality, and serde renames mirror upstream, so code compiles
  identically either way.

The stand-ins are **not** wire-compatibility guarantees against a real
apiserver — they exist so `Resource`, `ResourceExt`, `ObjectRef`, `Store`,
and the predicates work for your own types. Lean-gated as whole units:
the `crd` module, the apps/v1 `Restart` impls, the autoscaling `Scale`
re-export, and the blanket `impl Resource for K: k8s_openapi::Metadata`.
`admission` and `cel` now imply `k8s-openapi`.

Giving your type real(ish) metadata is the whole trick, and it pays for
itself: put your natural key in `metadata.name` and your change token
(content hash, record CID, sequence number) in `metadata.resource_version`,
and the upstream `predicates::resource_version` no-op filter, `ObjectRef`
identity, and `Store` keying all work with zero adaptation.

### kube-runtime

```toml
default = ["client", "events", "finalizer", "wait"]
client    = ["dep:kube-client", "kube-core/k8s-openapi"]
events    = ["client", "dep:k8s-openapi", "dep:hostname"]
finalizer = ["client", "dep:json-patch"]
wait      = ["client"]
```

`kube-runtime` now depends on `kube-core` directly (imports were rerouted
from `kube_client::` re-exports), so a lean build never touches
`kube-client`. The K8s watcher FSM, `watcher()`/`metadata_watcher()`/
`watch_object()`, `events`, `finalizer`, and `wait` sit behind their flags;
`watcher::Event`, `watcher::Error`, `watcher::Config`, and the backoff types
stay lean because everything downstream speaks them.

## Layer 2 (strategy A): `Controller` without a client

The `Controller` builder was the biggest casualty of turning `client` off,
and its Api-coupling turned out to be confined to constructors. So:

- The **`Controller` struct, `run()`, `reconcile_all_on`,
  `graceful_shutdown_on`/`shutdown_on_signal`, `trigger_backoff`,
  `with_config`, `store`** are available in lean builds.
- The **stream constructors are stabilized** (upstream keeps them behind
  `unstable-runtime-stream-control`): `for_stream`, `for_stream_with`,
  `owns_stream`, `owns_stream_with`, `watches_stream`, `watches_stream_with`.
  They take `Stream<Item = Result<K, watcher::Error>>` — bring your own
  stream, get the full builder ergonomics.
- Only **`new`/`new_with`/`owns`/`owns_with`/`watches`/`watches_with`**
  (the `Api`-driven constructors) remain behind `client`.
- **`watcher::Error::Source(Box<dyn Error + Send + Sync>)`** is a new
  variant (all builds) so foreign transports flow their errors through the
  existing plumbing — reflectors, backoff combinators, `Controller` trigger
  streams — without impersonating an apiserver `Status`.
- tokio features `rt` + `signal` are now required by the crate (for
  `run()`'s task spawning and `shutdown_on_signal`).

## Layer 3 (strategy B): `WatchSource` + `source_watcher`

The K8s watcher is a state machine over two operations — paginated initial
list, resumable watch — plus a handful of protocol signals. Everything else
is Kubernetes encoding:

| inside kube's watcher | the general concept | in `WatchSource` |
|---|---|---|
| `resource_version` | resume cursor | `type Cursor` |
| `continue_token` | page token | `type PageToken` / `ListPage::next_page` |
| `WatchEvent::Bookmark` | cursor advance without data | `WatchStep::Cursor` |
| HTTP 410 GONE | desync → discard cursor, re-list | `ErrorClass::Desync` |
| HTTP 403 | denied → surface loudly | `ErrorClass::Denied` |
| `next_with_idle_timeout` | liveness timeout → reconnect | `idle_timeout()` |

The new **`kube_runtime::source`** module (always available) captures the
signals without the encoding:

```rust
pub trait WatchSource {
    type Value: Clone + Send + 'static;   // impl kube_core::Resource for Store/Controller use
    type Cursor: Clone + Send;            // seq number, GTID, CID, mtime+hash, ...
    type PageToken: Send;
    type Error: std::error::Error + Send + Sync + 'static;
    type WatchStream: Stream<Item = Result<WatchStep<Self::Value, Self::Cursor>, Self::Error>> + Send + Unpin;

    async fn list(&self, page: Option<Self::PageToken>) -> Result<ListPage<...>, Self::Error>;
    async fn watch(&self, from: Self::Cursor) -> Result<Self::WatchStream, Self::Error>;
    fn classify(&self, err: &Self::Error) -> ErrorClass;   // Desync | Denied | Transient
    fn idle_timeout(&self) -> Option<Duration>;            // default: trust the stream
}
```

**`source_watcher(source)`** drives the same FSM shape as the K8s watcher —
`Event::Init` → `InitApply`×n (across pages) → `InitDone`, then
`Apply`/`Delete`; desync re-lists from scratch; a stream that ends or idles
out reconnects from the last cursor; transient errors surface as
`Error::Source` while the machine holds position (apply
`WatchStreamExt::default_backoff` downstream, exactly as for `watcher`).
It deliberately does **not** modify kube's own `step_trampolined` — the K8s
watcher remains the untouched, battle-tested driver for the K8s encoding,
which keeps upstream merges cheap.

Transport note: your HTTP/WebSocket/WebTransport client (tokio or
otherwise) lives *inside* your `WatchSource` impl. Abstracting at the
client level was considered and rejected — the FSM never cared about
transport, only about pages, cursors, and steps.

## End-to-end (verified)

This is the lean pipeline, run to completion with zero K8s dependencies in
the graph (abridged from the verification probe):

```rust
use kube_core::{k8s::ObjectMeta, ClusterResourceScope, Resource};
use kube_runtime::{
    controller::Action, reflector::{reflector, store::Writer},
    source::{ErrorClass, ListPage, WatchSource, WatchStep},
    source_watcher, Controller, WatchStreamExt,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Doc { metadata: ObjectMeta, body: String }
impl Resource for Doc { /* constants + meta()/meta_mut() over the embedded ObjectMeta */ }

struct DocSource; // wraps whatever client the real source needs
impl WatchSource for DocSource {
    type Value = Doc; type Cursor = u64; type PageToken = String;
    type Error = SourceError;
    type WatchStream = BoxStream<'static, Result<WatchStep<Doc, u64>, SourceError>>;
    async fn list(&self, _: Option<String>) -> Result<ListPage<Doc, u64, String>, SourceError> { /* ... */ }
    async fn watch(&self, from: u64) -> Result<Self::WatchStream, SourceError> { /* ... */ }
    fn classify(&self, e: &SourceError) -> ErrorClass { /* map cursor-too-old to Desync */ }
}

let writer = Writer::<Doc>::new(());
let reader = writer.as_reader();
let stream = reflector(writer, source_watcher(DocSource)).applied_objects().boxed();
Controller::for_stream(stream, reader)
    .run(reconcile, error_policy, ctx)
    .for_each(|res| async move { /* ... */ })
    .await;
```

## Numbers

Measured with probe crates outside the workspace (see the CI trap below),
debug profile, warm sccache-less machine:

| | default features | `default-features = false` |
|---|---|---|
| dependency graph | 147 packages | **83 packages** |
| clean debug build | 17.8 s | **6.9 s** |
| target dir | 810 M | **363 M** |
| k8s-openapi / kube-client / hyper / rustls / tower | present | **absent** |

## Divergences from upstream (merge checklist)

1. `kube-core`: `k8s-openapi` optional behind default-on feature; new
   `src/k8s.rs` seam; internal imports rerouted through it; `crd`/`util`
   impls/`Scale`/blanket-`Resource` gated; `admission`/`cel` imply
   `k8s-openapi`; jiff gains `serde`.
2. `kube-runtime`: `client`/`events`/`finalizer`/`wait` default-on features;
   direct `kube-core` dependency; imports rerouted from `kube_client::`
   re-exports; watcher FSM/`ApiMode`/entry points client-gated;
   `hostname`/`json-patch`/`k8s-openapi` optional; futures gains `std`;
   tokio gains `rt`+`signal`.
3. `Controller`: struct + `run` + stream constructors ungated; the six
   `-stream` constructors **stabilized** (upstream: `unstable-runtime-stream-control`,
   which remains as a no-op feature for compatibility).
4. `watcher::Error`: new `Source(Box<dyn Error + Send + Sync>)` variant.
   (Additive; note for anyone matching exhaustively.)
5. New module: `kube_runtime::source` (`WatchSource`, `WatchStep`,
   `ListPage`, `ErrorClass`, `source_watcher`), re-exported at crate root.
6. Lean-only `Error` shape: the three `kube_client::Error`-wrapping watcher
   error variants are client-gated, so lean matches see fewer variants.

Not touched: scheduler, runner, future_hash_map, delayed_init, reflector
internals, applier logic, kube-client, kube-derive, kube (facade defaults
unchanged — facade users see the full crate exactly as before).

## Verification & the resolver-1 CI trap

- Default features: full upstream test suites pass (`cargo test -p
  kube-core --lib`: 85; `-p kube-runtime --lib`: 83 = 78 upstream + 5 new
  `source::tests` covering init paging, desync re-list, stream-end
  reconnect, missing-cursor error, transient hold-position).
- Lean: probe crates build and the end-to-end pipeline above runs.

**Do not trust `cargo check --no-default-features` run inside this
workspace.** The workspace uses `resolver = "1"`, which unifies
dev-dependency features into the resolve — the dev-deps re-enable
kube-client and k8s-openapi and silently mask lean breakage (this bit us:
an external probe caught six real errors the in-workspace check passed).
Lean CI must build an out-of-tree consumer crate, or the workspace must
move to resolver 2.

## What lean builds give up

The K8s watch-protocol driver (`watcher()` and friends), the `Api`-driven
`Controller` constructors, `events::Recorder` (use tracing), `finalizer()`
(the concept — durably declare intent before acting — transplants to your
own store), `wait`, CRD/admission/scale machinery, and apiserver wire
compatibility of the metadata types. That is: everything whose meaning
*requires* a Kubernetes cluster on the other end.
