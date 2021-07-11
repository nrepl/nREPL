# Changelog

## master (unreleased)

### New features

* [#217](https://github.com/nrepl/nrepl/pull/217): Add keyword completion support.
* [#226](https://github.com/nrepl/nrepl/pull/226): Add doc and arglists to completion responses.
* [#238](https://github.com/nrepl/nrepl/pull/238): Expand completion and lookup error message when ns not found.
* [#217](https://github.com/nrepl/nrepl/pull/217): Add server support
  for UNIX domain (filesystem) sockets via `-s/--socket PATH` on the
  command line or `(start-server ... :socket PATH)` whenever the JDK
  is version 16 or newer or
  [junixsocket](https://kohlschutter.github.io/junixsocket/) is
  avaialble as a dependency.

### Bugs fixed

* [#227](https://github.com/nrepl/nrepl/pull/227): Fix completion for static class members.
* [#231](https://github.com/nrepl/nrepl/issues/231): Fix sanitize error when file is `java.net.URL`.

## 0.8.3 (2020-10-25)

### Bugs fixed

* [#213](https://github.com/nrepl/nrepl/pull/213): Fix sideloader race condition.

## 0.8.2 (2020-09-15)

### Bugs fixed

* [#211](https://github.com/nrepl/nrepl/pull/211): Fix a couple of lookup op errors.

## 0.8.1 (2020-09-03)

### Bugs fixed

* [#206](https://github.com/nrepl/nrepl/issues/206): Fix classloader-related breakage with `cider-nrepl` and Java 8.

## 0.8.0 (2020-08-01)

### New features

* Bundle a couple of print functions compatible with the print middleware (see `nrepl.util.print`).
* [#174](https://github.com/nrepl/nrepl/issues/174): Provide a built-in `completions` op.
* [#143](https://github.com/nrepl/nrepl/issues/143): Added a middleware that allows dynamic loading/unloading of middleware while the server is running.
* [#180](https://github.com/nrepl/nrepl/issues/180): Provide a built-in `lookup` op.

### Bugs fixed

* [#125](https://github.com/nrepl/nrepl/issues/125): The built-in client supports `greeting-fn`.
* [#126](https://github.com/nrepl/nrepl/issues/126): The built-in client exits with an error message when the tty transport is selected. It used to fail silently. This was never supported.
* [#113](https://github.com/nrepl/nrepl/issues/113): Fix an issue with hotloading using Pomegranate in Leiningen.
* [#17](https://github.com/nrepl/nrepl/issues/17): It was possible for the bencode transport to write partial messages if a middleware tries to write something unencodable. This could cause the client or server to hang.

## 0.7.0 (2020-03-28)

### New features

* [#60](https://github.com/nrepl/nrepl/issues/60): Implemented EDN transport.
* [#140](https://github.com/nrepl/nrepl/issues/140): Added initial version of spec for message responses. These are used during Clojure 1.10 tests.
* [#97](https://github.com/nrepl/nrepl/issues/97): Added a sideloader, a network classloader that allows dependencies to be added even when the source/class files are not available on the server JVM's classpath (e.g. supplied by the client).

### Bugs fixed

* [#152](https://github.com/nrepl/nrepl/issues/152): Kill session threads when closing sessions.
* [#132](https://github.com/nrepl/nrepl/issues/132): Avoid malformed bencode messages during interrupts, mainly affecting streamed printing.

### Changes

* [#137](https://github.com/nrepl/nrepl/pull/137): Expanded Bencode writer to work with
  maps that have keywords or symbols as keys. This allowed a simplification of the
  Bencode transport itself.
* [#158](https://github.com/nrepl/nrepl/issues/158): Interrupt now runs in three stages: calls `interrupt` on the thread, waits 100ms for the thread to respond and return messages, then waits 5000ms for the thread to terminate itself. A hard `.stop` is only called if it fails to do so.
* [#178](https://github.com/nrepl/nrepl/pull/178): Allow `:read-cond` option when evaluating code.
* [#167](https://github.com/nrepl/nrepl/issues/167): Allow suppressing ack message when using `nrepl.cmdline`.

## 0.6.0 (2019-02-05)

### New features

* [#117](https://github.com/nrepl/nrepl/issues/117): Replace
  `nrepl.middleware.pr-values` with `nrepl.middleware.print`.
  * New dynamic vars in `nrepl.middleware.print` for configuring the print
    middleware at the REPL.
  * The new middleware provides behaviour that is backwards-compatible with the
    old one. Existing middleware descriptors whose `:requires` set contains
    `#'pr-values` should instead use `#'wrap-print`.
* [#128](https://github.com/nrepl/nrepl/pull/128): New middleware,
  `nrepl.middleware.caught`, provides a hook called when eval, read, or print
  throws an exception or error. Defaults to `clojure.main/repl-caught`.
  Configurable by the dynamic var `nrepl.middleware.caught/*caught-fn*`.

### Bugs fixed

* [CLI] Make sure ack port parameter is converted to integer for command line nREPL initialization.
* [CLI] When starting the REPL, make sure the transport option is used correctly.
* [CLI] Make sure calling `send-ack` at `cmdline` ns works with the correct transport.
* [#8](https://github.com/nrepl/nrepl/issues/8): Clean up context classloader after eval.

### Changes

* [#16](https://github.com/nrepl/nrepl/issues/16): Use a single session thread per evaluation.
* [#107](https://github.com/nrepl/nrepl/issues/107): Stop reading and evaluating code on first read error.
* [#108](https://github.com/nrepl/nrepl/issues/108): Refactor cmdline functions into a public, reusable API.
* Restore the `nrepl.bencode` namespace.

## 0.5.3 (2018-12-12)

### Bugs fixed

* Make sure we never send a nil transport to via `send-ack`.

## 0.5.2 (2018-12-10)

### Bugs fixed

* [CLI] [#90](https://github.com/nrepl/nrepl/issues/90): Doesn't display properly URLs if using a 3rd-party transport.

## 0.5.1 (2018-11-30)

### Changes

* [#89](https://github.com/nrepl/nrepl/issues/89): Remove `tools.logging` dependency.

## 0.5.0 (2018-11-28)

### New features

* [#12](https://github.com/nrepl/nrepl/issues/12): Support custom printing
  function in `pr-values`, enabling pretty-printed REPL results.
* [#66](https://github.com/nrepl/nrepl/pull/66): Add support for a global and local configuration file.
* [CLI] [#63](https://github.com/nrepl/nrepl/issues/63): Make it possible to specify the transport via the command-line client (`--transport/-t`).

### Bugs fixed

* [#10](https://github.com/nrepl/nrepl/issues/10): Bind `*1`, `*2`, `*3` and `*e` in cloned session.
* [#33](https://github.com/nrepl/nrepl/issues/33): Add ability to change value of `*print-namespace-maps*`.
* [#68](https://github.com/nrepl/nrepl/issues/68): Avoid illegal access warning on JDK 9+ caused by `nrepl.middleware.interruptible-eval/set-line!`.
* [CLI] [#77](https://github.com/nrepl/nrepl/issues/77): Exit cleanly after pressing `ctrl-d` in an interactive REPL.
* [#13](https://github.com/nrepl/nrepl/issues/13): Catch ThreadDeath exception thrown by interrupt.

### Changes

* [#56](https://github.com/nrepl/nrepl/issues/56): Bind the server by default to `127.0.0.1` instead of to `::` (this turned out to be a security risk).
* [#76](https://github.com/nrepl/nrepl/pull/76): Move version-related logic to a dedicated namespace (`nrepl.version`).
* Deprecate `nrepl.core/version`.
* Deprecate `nrepl.core/version-string`.
* [CLI] [#81](https://github.com/nrepl/nrepl/pull/81): Handle interrupt in interactive session.

## 0.4.5 (2018-09-02)

### New features

* [CLI] The built-in the CLI generates an `.nrepl-port` file on server startup.
* [CLI] [#39](https://github.com/nrepl/nrepl/issues/39): Add a `--connect` command-line option allowing you to connect.
with the built-in client to an already running nREPL server.
* [CLI] Add shorthand names for most command-line options.
* [CLI] Add a `-v/--version` command-line option.

### Changes

* [#32](https://github.com/nrepl/nrepl/issues/32): Extract the bencode logic in a [separate library](https://github.com/nrepl/bencode).

### Bugs fixed

* [#38](https://github.com/nrepl/nrepl/issues/38): Remove extra newline in REPL output.

## 0.4.4 (2018-07-31)

### New features

* [CLI] Added `--help` command-line option.
* [CLI] Added `--bind` command-line option.
* [CLI] Added `--handler` and `--middleware` command-line options. Extremely useful when starting nREPL using
`clj` and `tools.deps`, as this allows you to inject middleware trivially without the need for
something like `lein` or `boot`.

### Bugs fixed

* [CLI] Add missing newline after colorized values displayed in the REPL.

## 0.4.3 (2018-07-26)

### New features

* [CLI] Display connection info when starting the built-in cmd client. This makes it possible
for clients like CIDER to parse it and auto-connect to the server. Pretty handy if you're
using `clj` to start your server.

### Bugs fixed

* [#16](https://github.com/nrepl/nrepl/issues/16): Don't change the
  thread used for form evaluation over time. See
  [#36](https://github.com/nrepl/nrepl/pull/36) for a discussion of
  the fix. **(partial fix)**

### Changes

* The result of `nrepl.server/start-server` no longer contains the
legacy key `:ss` from the days of nREPL 0.0.x. If someone was using it
they should switch to `:server-socket` instead.
* [#28](https://github.com/nrepl/nrepl/issues/28): Echo back missing
ns during eval (previously you'd only get an error that a ns is missing,
but no mention of the name of that namespace).

## 0.4.2 (2018-07-18)

### Changes

* [#35](https://github.com/nrepl/nrepl/pull/35): Add constant DCL
across evaluations (which means you can now easily hot-load
dependencies).

### Bugs fixed

* [#34](https://github.com/nrepl/nrepl/pull/34): Treat `nil` port as 0
  (which assigns a random port).

## 0.4.1 (2018-05-23)

### Bugs fixed

* [#11](https://github.com/nrepl/nrepl/issues/11): Don't read the version string
from a resource file (`version.txt`).

## 0.4.0 (2018-05-21)

### Changes

* [#4](https://github.com/nrepl/nrepl/issues/4): Change the project's
  namespaces. (**breaking**) `clojure.tools.nrepl` is now `nrepl.core`,
  the rest of the namespaces were renamed following the pattern
  `clojure.tools.nrepl.*` -> `nrepl.*`.

## 0.3.1 (2018-05-19)

### Bugs fixed

* [#15](https://github.com/nrepl/nrepl/issues/15) Fix for
  `clojure.tools.nrepl.middleware.session` for `:unknown-session`
  error and `clojure.tools.nrepl.middleware.interruptible-eval` for
  `:no-code` error, the correct response of `:status :done` is now
  being returned.
* [#26](https://github.com/nrepl/nrepl/issues/26): Recompile the Java classes
for Java 8.

## 0.3.0 (2018-05-18)

### Changes

* [#1](https://github.com/nrepl/nrepl/issues/1): Materially identical
  to `[org.clojure/tools.nrepl "0.2.13"]`, but released under
  `nrepl/nrepl` coordinates as part of the migration out of
  clojure-contrib https://github.com/nrepl/nrepl
* `clojure.tools/logging` is now a normal dependency (it used to be an
  optional dependency).

### Bugs fixed

* [#20](https://github.com/nrepl/nrepl/issues/20): If `start-server` is not provided with a `:bind` hostname, nREPL will default
  to binding to the ipv6 `::` (as before), but will now _always_ fall back to
  `localhost`. Previously, the ipv4 hostname was only used if `::` could not be
  resolved; this change ensures that the `localhost` fallback is used in
  networking environments where `::` is resolved successfully, but cannot be
  bound.

--------------------------------------------------------------------------------

`0.2.13`:

* `start-server` now binds to `::` by default, and falls back to `localhost`,
  avoiding confusion when working in environments that have both IPv4 and IPv6
  networking available. (NREPL-83)

`0.2.11`:

* `clojure.tools.nrepl.middleware.interruptible-eval` now accepts optional
  `file`, `line`, and `column` values in order to fix location metadata to
  defined vars and functions, for more useful stack traces, navigation, etc.
* REPL evaluations now support use of reader conditionals (loading `.cljc` files
  containing reader conditionals has always worked transparently)

`0.2.10`:

* `clojure.tools.nrepl.middleware.pr-values` will _not_ print the contents of
  `:value` response messages if the message contains a `:printed-value` slot.
* `default-executor` and `queue-eval` in
  `clojure.tools.nrepl.middleware.interruptible-eval` are now public.

`0.2.9`:

* `clojure.tools.nrepl.middleware.interruptible-eval` now defines a default
  thread executor used for all evaluations (unless a different executor is
  provided to the configuration of
  `clojure.tools.nrepl.middleware.interruptible-eval/interruptible-eval`). This
  should aid in the development of `interrupt`-capable alternative evaluation
  middlewares/handlers.

`0.2.8`:

* The default bind address used by `clojure.tools.nrepl.server/start-server` is
  now `localhost`, not `0.0.0.0`. As always, the bind address can be set
  explicitly via a `:bind` keyword argument to that function. This is considered
  a security bugfix, though _technically_ it may cause breakage if anyone was
  implicitly relying upon nREPL's socket server to listen on all network
  interfaces.
* The `ServerSocket` created as part of
  `clojure.tools.nrepl.server/start-server` is now configured with
  `SO_REUSEADDR` enabled; this should prevent spurious "address already in use"
  when quickly bouncing apps that open an nREPL server on a fixed port, etc.
  (NREPL-67)
* Middlewares may now contribute to the response of the `"describe"` operation
  via an optional `:describe-fn` function provided via their descriptors.
  (NREPL-64)
* The `:ns` component of the response to `"load-file"` operations is now elided,
  as it was (usually) incorrect (as a result of reusing `interruptible-eval` for
  handling `load-file` operations) (NREPL-68)

`0.2.7`:

* The topological sort ("linearization") applied to middleware provided to start
  a new nREPL server has been reworked to address certain edge case bugs
  (NREPL-53)
* `interruptible-eval` no longer incorrectly clobbers a session's `*ns*` binding
  when it processes an `eval` message containing an `ns` "argument"
* Eliminated miscellaneous reflection warnings

`0.2.5`:

* Clients can now signal EOF on `*in*` with an empty `:stdin` value (NREPL-65)
* Clojure `:version-string` is now included in response to a `describe`
  operation (NREPL-63)
* Improve representation of `java.version` information in response to a
  `describe` operation (NREPL-62)

`0.2.4`:

* Fixed the source of a reliable per-connection thread leak (NREPL-40)
* Fix printing of lazy sequences so that `*out*` bindings are properly preserved
  (NREPL-45)
* Enhance `clojure.tools.nrepl.middleware.interruptible-eval/evaluate` so that a
  custom `eval` function can be provided on a per-message basis (NREPL-50)
* Fix pretty-printing of reference returned by
  `clojure.tools.nrepl.server/start-server` (NREPL-51)
* nREPL now works with JDK 1.8 (NREPL-56)
* The value of the `java.version` system property is now included in the response
  to a `describe` operation (NREPL-57)
* Common session bindings (e.g. `*e`, `*1`, etc) are now set in time for nREPL
  middleware to access them in the case of an exception being thrown (NREPL-58)

`0.2.3`:

* Now using a queue to maintain `*in*`, to avoid intermittent failures due to
  prior use of `PipedReader`/`Writer`. (NREPL-39)
* When loading a file, always bind `*print-level*` and `*print-length*` when
  generating the `clojure.lang.Compiler/load` expression (NREPL-41)

`0.2.2`:

* Added `clojure.tools.nrepl/code*` for `pr-str`'ing expressions (presumably
  for later evaluation)
* session IDs are now properly combined into a set by
  `clojure.tools.nrepl/combine-responses`
* fixes printing of server instances under Clojure 1.3.0+ (nREPL-37)

`0.2.1`:

* fixes incorrect translation between `Writer.write()` and
  `StringBuilder.append()` APIs (NREPL-38)

`0.2.0`:

Top-to-bottom redesign

`0.0.6`:

Never released; initial prototype of "rich content" support that (in part)
helped motivate a re-examination of the underlying protocol and design.

`0.0.5`:

- added Clojure 1.3.0 (ALPHA) compatibility

`0.0.4`:

- fixed (hacked) obtaining `clojure.test` output when `clojure.test` is
  initially loaded within an nREPL session
- eliminated 1-minute default timeout on expression evaluation
- all standard REPL var bindings are now properly established and maintained
  within a session
