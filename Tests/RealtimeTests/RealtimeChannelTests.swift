//
//  RealtimeChannelTests.swift
//  Supabase
//
//  Created by Guilherme Souza on 09/09/24.
//

import ConcurrencyExtras
import Foundation
import InlineSnapshotTesting
import TestHelpers
import XCTest
import XCTestDynamicOverlay

@testable import Realtime
@testable import RealtimeV2

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

final class RealtimeChannelTests: XCTestCase {
  let sut = RealtimeChannelV2(
    topic: "topic",
    config: RealtimeChannelConfig(
      broadcast: BroadcastJoinConfig(),
      presence: PresenceJoinConfig(),
      isPrivate: false
    ),
    socket: RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(headers: ["apikey": "test-key"])
    ),
    logger: nil
  )

  @MainActor
  func testSubscribeWithErrorClassifiesOnlyKnownTransientTransportFailures() async {
    for code in [URLError.Code.notConnectedToInternet, .networkConnectionLost] {
      let transportCalls = LockIsolated(0)
      let socket = makeSocket(maxRetryAttempts: 1) { _, _ in
        transportCalls.withValue { $0 += 1 }
        throw WebSocketError.connection(
          message: "connection failed",
          error: URLError(code)
        )
      }

      do {
        try await socket.channel("test-topic").subscribeWithError()
        XCTFail("Expected a transient transport failure for \(code)")
      } catch let error as RealtimeSubscribeError {
        XCTAssertEqual(error, .transientTransportFailure)
      } catch {
        XCTFail("Unexpected error for \(code): \(error)")
      }

      XCTAssertEqual(transportCalls.value, 1)
      guard case .disconnected = socket.status else {
        return XCTFail("Expected the socket to remain disconnected")
      }
    }
  }

  @MainActor
  func testSubscribeWithErrorDoesNotClassifyTimeoutHandshakeOrUnknownFailuresAsTransient()
    async
  {
    let failures: [any Error] = [
      WebSocketError.connection(message: "connection failed", error: URLError(.timedOut)),
      WebSocketError.connection(message: "connection failed", error: URLError(.cancelled)),
      WebSocketError.connection(
        message: "connection failed",
        error: URLError(.userAuthenticationRequired)
      ),
      WebSocketError.connection(message: "connection failed", error: URLError(.badServerResponse)),
      WebSocketError.connection(
        message: "connection failed",
        error: NSError(domain: "UnknownTransport", code: 1)
      ),
    ]

    for failure in failures {
      let transportCalls = LockIsolated(0)
      let socket = makeSocket(maxRetryAttempts: 5) { _, _ in
        transportCalls.withValue { $0 += 1 }
        throw failure
      }

      do {
        try await socket.channel("test-topic").subscribeWithError()
        XCTFail("Expected the connection failure to be surfaced")
      } catch {
        XCTAssertFalse(error is RealtimeSubscribeError)
      }

      XCTAssertEqual(transportCalls.value, 1)
      guard case .disconnected = socket.status else {
        return XCTFail("Expected the socket to remain disconnected")
      }
    }
  }

  @MainActor
  func testPublicConnectRemainsNonThrowingForTransportFailure() async {
    let socket = makeSocket(maxRetryAttempts: 1) { _, _ in
      throw WebSocketError.connection(
        message: "connection failed",
        error: URLError(.notConnectedToInternet)
      )
    }

    await socket.connect()

    guard case .disconnected = socket.status else {
      return XCTFail("Expected public connect() to remain non-throwing and disconnected")
    }
  }

  @MainActor
  func testSubscribeWithErrorReturnsCorrelatedJoinRejectionWithoutRetry() async {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 5) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: message.ref,
            topic: message.topic,
            event: "phx_reply",
            payload: [
              "status": "error",
              "response": ["reason": "private authorization detail"],
            ]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    do {
      try await channel.subscribeWithError()
      XCTFail("Expected the server rejection to be surfaced")
    } catch let error as RealtimeSubscribeError {
      XCTAssertEqual(error, .joinRejected)
      XCTAssertFalse(error.localizedDescription.contains("private authorization detail"))
    } catch {
      XCTFail("Unexpected error: \(error)")
    }

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .unsubscribed)
  }

  @MainActor
  func testMalformedCorrelatedJoinReplyFailsClosedWithoutRetry() async {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 5) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: message.ref,
            topic: message.topic,
            event: "phx_reply",
            payload: ["response": ["reason": "must not be exposed"]]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    do {
      try await channel.subscribeWithError()
      XCTFail("Expected the malformed join reply to fail")
    } catch {
      XCTAssertFalse(error is RealtimeSubscribeError)
      XCTAssertFalse(error.localizedDescription.contains("must not be exposed"))
    }

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .unsubscribed)
  }

  @MainActor
  func testJoinReplyMissingPushRefFailsClosedWithoutRetry() async {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 5) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: nil,
            topic: message.topic,
            event: "phx_reply",
            payload: ["status": "error", "response": [:]]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    do {
      try await channel.subscribeWithError()
      XCTFail("Expected the malformed join reply to fail")
    } catch {
      XCTAssertFalse(error is RealtimeSubscribeError)
    }

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .unsubscribed)
  }

  @MainActor
  func testJoinErrorWithoutStructuredResponseIsNotClassifiedAsRejection() async {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 5) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: message.ref,
            topic: message.topic,
            event: "phx_reply",
            payload: ["status": "error"]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    do {
      try await channel.subscribeWithError()
      XCTFail("Expected the malformed join error to fail")
    } catch {
      XCTAssertFalse(error is RealtimeSubscribeError)
    }

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .unsubscribed)
  }

  @MainActor
  func testSystemTimeoutDuringJoinIsNotClassifiedAsRejection() async {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 5) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: nil,
            topic: message.topic,
            event: ChannelEvent.system,
            payload: ["status": "timeout"]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    do {
      try await channel.subscribeWithError()
      XCTFail("Expected the system timeout to fail the subscribe")
    } catch {
      XCTAssertFalse(error is RealtimeSubscribeError)
    }

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .unsubscribed)
  }

  @MainActor
  func testStaleJoinRejectionDoesNotBlockCurrentCorrelatedJoin() async throws {
    let (client, server) = FakeWebSocket.fakes()
    let joinCount = LockIsolated(0)
    let socket = makeSocket(maxRetryAttempts: 1) { _, _ in client }
    let channel = socket.channel("test-topic")

    let serverTask = Task { @Sendable in
      for await event in server.events {
        guard let message = event.realtimeMessage, message.event == "phx_join" else {
          continue
        }
        joinCount.withValue { $0 += 1 }
        server.send(
          RealtimeMessageV2(
            joinRef: "stale-join",
            ref: "stale-join",
            topic: message.topic,
            event: "phx_reply",
            payload: ["status": "error", "response": [:]]
          )
        )
        server.send(
          RealtimeMessageV2(
            joinRef: message.joinRef,
            ref: message.ref,
            topic: message.topic,
            event: "phx_reply",
            payload: ["status": "ok", "response": ["postgres_changes": []]]
          )
        )
      }
    }
    defer {
      serverTask.cancel()
      socket.disconnect()
    }

    try await channel.subscribeWithError()

    XCTAssertEqual(joinCount.value, 1)
    XCTAssertEqual(channel.status, .subscribed)
  }

  @MainActor
  func testReconnectDuringAccessTokenLookupDoesNotSendObsoleteJoin() async throws {
    let tokenGate = TokenGate()
    let (client, server) = FakeWebSocket.fakes()
    let socket = makeSocket(
      maxRetryAttempts: 1,
      accessToken: { await tokenGate.token() },
      wsTransport: { _, _ in client }
    )
    let channel = socket.channel("test-topic")

    let firstSubscribe = Task { try await channel.subscribeWithError() }
    guard await tokenGate.waitForRequestCount(1) else {
      firstSubscribe.cancel()
      await tokenGate.resumeAll(with: nil)
      _ = await firstSubscribe.result
      return XCTFail("The initial join should request an access token")
    }
    let staleJoinRef = await channel.stateManager.joinRef!

    await channel.resetForReconnect()

    let secondSubscribe = Task { try await channel.subscribeWithError() }
    guard await tokenGate.waitForRequestCount(2) else {
      firstSubscribe.cancel()
      secondSubscribe.cancel()
      await tokenGate.resumeAll(with: nil)
      _ = await firstSubscribe.result
      _ = await secondSubscribe.result
      return XCTFail("The rejoined channel should request a fresh access token")
    }
    let currentJoinRef = await channel.stateManager.joinRef!
    XCTAssertNotEqual(staleJoinRef, currentJoinRef)

    await tokenGate.resumeNext(with: "stale-token")
    do {
      try await firstSubscribe.value
      XCTFail("The old subscribe must be cancelled by reconnect reset")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Unexpected stale subscribe error: \(error)")
    }

    XCTAssertTrue(
      client.sentEvents.compactMap(\.realtimeMessage).allSatisfy { $0.event != "phx_join" },
      "The stale access-token continuation must not send an old join"
    )

    await tokenGate.resumeNext(with: "current-token")
    let joins = await waitForSentEvents(in: client, event: "phx_join", timeout: 1)
    XCTAssertEqual(joins.map(\.ref), [currentJoinRef])

    server.send(
      RealtimeMessageV2(
        joinRef: staleJoinRef,
        ref: staleJoinRef,
        topic: "realtime:test-topic",
        event: "phx_reply",
        payload: ["status": "error", "response": ["reason": "stale denial"]]
      )
    )
    server.send(
      RealtimeMessageV2(
        joinRef: currentJoinRef,
        ref: currentJoinRef,
        topic: "realtime:test-topic",
        event: "phx_reply",
        payload: ["status": "ok", "response": ["postgres_changes": []]]
      )
    )
    try await secondSubscribe.value
  }

  func testTypedFilterAndSelectAreBufferedIntoPostgresJoinConfig() {
    let subscription = sut.onPostgresChange(
      UpdateAction.self,
      table: "orders",
      filter: .and([
        .gt("amount", value: 100),
        .not(.in("status", values: ["draft"])),
      ]),
      select: ["id", "name"]
    ) { _ in }
    defer { subscription.cancel() }

    let changes = sut.clientChanges.value
    XCTAssertEqual(changes.count, 1)
    XCTAssertEqual(changes.first?.event, .update)
    XCTAssertEqual(changes.first?.table, "orders")
    XCTAssertEqual(changes.first?.filter, "amount=gt.100,status=not.in.(draft)")
    XCTAssertEqual(changes.first?.select, ["id", "name"])
  }

  // MARK: - Callback rejection tests

  #if canImport(Darwin)
    @MainActor
    func testPresenceChangeCallbackRejectedWhileSubscribing() async {
      let (client, server) = FakeWebSocket.fakes()
      let socket = RealtimeClientV2(
        url: URL(string: "https://localhost:54321/realtime/v1")!,
        options: RealtimeClientOptions(
          headers: ["apikey": "test-key"],
          accessToken: { "test-token" }
        ),
        wsTransport: { _, _ in client },
        http: HTTPClientMock()
      )

      let channel = socket.channel("test-topic")

      // Never respond to phx_join, so channel stays in .subscribing
      let serverTask1 = Task { @Sendable [server] in
        for await event in server.events {
          guard let msg = event.realtimeMessage else { continue }
          if msg.event == "heartbeat" {
            server.send(
              RealtimeMessageV2(
                joinRef: msg.joinRef,
                ref: msg.ref,
                topic: "phoenix",
                event: "phx_reply",
                payload: ["response": [:]]
              )
            )
          }
        }
      }
      defer { serverTask1.cancel() }

      await socket.connect()

      let subscribeTask = Task { try? await channel.subscribeWithError() }

      await waitForChannelStatus(.subscribing, channel: channel, timeout: 2.0)
      XCTAssertEqual(channel.status, .subscribing)

      let callbackCountBefore = channel.callbackManager.callbacks.count

      withExpectedIssue {
        _ = channel.onPresenceChange { _ in }
      }

      XCTAssertEqual(channel.callbackManager.callbacks.count, callbackCountBefore)

      subscribeTask.cancel()
      socket.disconnect()
    }
  #endif

  #if canImport(Darwin)
    @MainActor
    func testPresenceChangeCallbackRejectedWhileSubscribed() async {
      let (client, server) = FakeWebSocket.fakes()
      let socket = RealtimeClientV2(
        url: URL(string: "https://localhost:54321/realtime/v1")!,
        options: RealtimeClientOptions(
          headers: ["apikey": "test-key"],
          accessToken: { "test-token" }
        ),
        wsTransport: { _, _ in client },
        http: HTTPClientMock()
      )

      let channel = socket.channel("test-topic")

      let serverTask2 = Task { @Sendable [server] in
        for await event in server.events {
          guard let msg = event.realtimeMessage else { continue }
          if msg.event == "phx_join" {
            server.send(
              RealtimeMessageV2(
                joinRef: msg.joinRef,
                ref: msg.ref,
                topic: "realtime:test-topic",
                event: "phx_reply",
                payload: [
                  "response": ["postgres_changes": []],
                  "status": "ok",
                ]
              )
            )
          }
        }
      }
      defer { serverTask2.cancel() }

      await socket.connect()
      try? await channel.subscribeWithError()
      XCTAssertEqual(channel.status, .subscribed)

      let callbackCountBefore = channel.callbackManager.callbacks.count

      withExpectedIssue {
        _ = channel.onPresenceChange { _ in }
      }

      XCTAssertEqual(channel.callbackManager.callbacks.count, callbackCountBefore)

      socket.disconnect()
    }
  #endif

  #if canImport(Darwin)
    @MainActor
    func testPostgresChangeCallbackRejectedWhileSubscribing() async {
      let (client, server) = FakeWebSocket.fakes()
      let socket = RealtimeClientV2(
        url: URL(string: "https://localhost:54321/realtime/v1")!,
        options: RealtimeClientOptions(
          headers: ["apikey": "test-key"],
          accessToken: { "test-token" }
        ),
        wsTransport: { _, _ in client },
        http: HTTPClientMock()
      )

      let channel = socket.channel("test-topic")

      // Never respond to phx_join, so channel stays in .subscribing
      let serverTask3 = Task { @Sendable [server] in
        for await event in server.events {
          guard let msg = event.realtimeMessage else { continue }
          if msg.event == "heartbeat" {
            server.send(
              RealtimeMessageV2(
                joinRef: msg.joinRef,
                ref: msg.ref,
                topic: "phoenix",
                event: "phx_reply",
                payload: ["response": [:]]
              )
            )
          }
        }
      }
      defer { serverTask3.cancel() }

      await socket.connect()

      let subscribeTask = Task { try? await channel.subscribeWithError() }

      await waitForChannelStatus(.subscribing, channel: channel, timeout: 2.0)
      XCTAssertEqual(channel.status, .subscribing)

      let callbackCountBefore = channel.callbackManager.callbacks.count

      withExpectedIssue {
        _ = channel.onPostgresChange(AnyAction.self, schema: "public") { _ in }
      }

      XCTAssertEqual(channel.callbackManager.callbacks.count, callbackCountBefore)

      subscribeTask.cancel()
      socket.disconnect()
    }
  #endif

  #if canImport(Darwin)
    @MainActor
    func testPostgresChangeCallbackRejectedWhileSubscribed() async {
      let (client, server) = FakeWebSocket.fakes()
      let socket = RealtimeClientV2(
        url: URL(string: "https://localhost:54321/realtime/v1")!,
        options: RealtimeClientOptions(
          headers: ["apikey": "test-key"],
          accessToken: { "test-token" }
        ),
        wsTransport: { _, _ in client },
        http: HTTPClientMock()
      )

      let channel = socket.channel("test-topic")

      let serverTask4 = Task { @Sendable [server] in
        for await event in server.events {
          guard let msg = event.realtimeMessage else { continue }
          if msg.event == "phx_join" {
            server.send(
              RealtimeMessageV2(
                joinRef: msg.joinRef,
                ref: msg.ref,
                topic: "realtime:test-topic",
                event: "phx_reply",
                payload: [
                  "response": ["postgres_changes": []],
                  "status": "ok",
                ]
              )
            )
          }
        }
      }
      defer { serverTask4.cancel() }

      await socket.connect()
      try? await channel.subscribeWithError()
      XCTAssertEqual(channel.status, .subscribed)

      let callbackCountBefore = channel.callbackManager.callbacks.count

      withExpectedIssue {
        _ = channel.onPostgresChange(AnyAction.self, schema: "public") { _ in }
      }

      XCTAssertEqual(channel.callbackManager.callbacks.count, callbackCountBefore)

      socket.disconnect()
    }
  #endif

  func testAttachCallbacks() {
    var subscriptions = Set<RealtimeSubscription>()

    sut.onPostgresChange(
      AnyAction.self,
      schema: "public",
      table: "users",
      filter: "id=eq.1"
    ) { _ in }.store(in: &subscriptions)
    sut.onPostgresChange(
      InsertAction.self,
      schema: "private"
    ) { _ in }.store(in: &subscriptions)
    sut.onPostgresChange(
      UpdateAction.self,
      table: "messages"
    ) { _ in }.store(in: &subscriptions)
    sut.onPostgresChange(
      DeleteAction.self
    ) { _ in }.store(in: &subscriptions)

    sut.onBroadcast(event: "test") { _ in }.store(in: &subscriptions)
    sut.onBroadcast(event: "cursor-pos") { _ in }.store(in: &subscriptions)

    sut.onPresenceChange { _ in }.store(in: &subscriptions)

    sut.onSystem {
    }
    .store(in: &subscriptions)

    assertInlineSnapshot(of: sut.callbackManager.callbacks, as: .dump) {
      """
      ▿ 8 elements
        ▿ RealtimeCallback
          ▿ postgres: PostgresCallback
            - callback: (Function)
            ▿ filter: PostgresJoinConfig
              ▿ event: Optional<PostgresChangeEvent>
                - some: PostgresChangeEvent.all
              ▿ filter: Optional<String>
                - some: "id=eq.1"
              - id: 0
              - schema: "public"
              - select: Optional<Array<String>>.none
              ▿ table: Optional<String>
                - some: "users"
            - id: 1
        ▿ RealtimeCallback
          ▿ postgres: PostgresCallback
            - callback: (Function)
            ▿ filter: PostgresJoinConfig
              ▿ event: Optional<PostgresChangeEvent>
                - some: PostgresChangeEvent.insert
              - filter: Optional<String>.none
              - id: 0
              - schema: "private"
              - select: Optional<Array<String>>.none
              - table: Optional<String>.none
            - id: 2
        ▿ RealtimeCallback
          ▿ postgres: PostgresCallback
            - callback: (Function)
            ▿ filter: PostgresJoinConfig
              ▿ event: Optional<PostgresChangeEvent>
                - some: PostgresChangeEvent.update
              - filter: Optional<String>.none
              - id: 0
              - schema: "public"
              - select: Optional<Array<String>>.none
              ▿ table: Optional<String>
                - some: "messages"
            - id: 3
        ▿ RealtimeCallback
          ▿ postgres: PostgresCallback
            - callback: (Function)
            ▿ filter: PostgresJoinConfig
              ▿ event: Optional<PostgresChangeEvent>
                - some: PostgresChangeEvent.delete
              - filter: Optional<String>.none
              - id: 0
              - schema: "public"
              - select: Optional<Array<String>>.none
              - table: Optional<String>.none
            - id: 4
        ▿ RealtimeCallback
          ▿ broadcast: BroadcastCallback
            - callback: (Function)
            - event: "test"
            - id: 5
        ▿ RealtimeCallback
          ▿ broadcast: BroadcastCallback
            - callback: (Function)
            - event: "cursor-pos"
            - id: 6
        ▿ RealtimeCallback
          ▿ presence: PresenceCallback
            - callback: (Function)
            - id: 7
        ▿ RealtimeCallback
          ▿ system: SystemCallback
            - callback: (Function)
            - id: 8

      """
    }
  }

  private func makeSocket(
    maxRetryAttempts: Int,
    accessToken: (@Sendable () async throws -> String?)? = nil,
    wsTransport: @escaping WebSocketTransport
  ) -> RealtimeClientV2 {
    RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        timeoutInterval: 0.1,
        maxRetryAttempts: maxRetryAttempts,
        accessToken: accessToken,
        handleAppLifecycle: false
      ),
      wsTransport: wsTransport,
      http: HTTPClientMock()
    )
  }

  @MainActor
  func testPresenceEnabledDuringSubscribe() async {
    // Create fake WebSocket for testing
    let (client, server) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: HTTPClientMock()
    )

    // Create a channel without presence callback initially
    let channel = socket.channel("test-topic")

    // Initially presence should be disabled
    XCTAssertFalse(channel.config.presence.enabled)

    // Connect the socket
    await socket.connect()

    // Add a presence callback before subscribing
    let presenceSubscription = channel.onPresenceChange { _ in }

    // Verify that presence callback exists
    XCTAssertTrue(channel.callbackManager.callbacks.contains(where: { $0.isPresence }))

    // Start subscription process
    Task {
      try? await channel.subscribeWithError()
    }

    // Wait for the join message to be sent
    let joinEvents = await waitForEvents(
      in: server,
      event: "phx_join",
      timeout: 1.0
    )

    // Should have at least one join event
    XCTAssertGreaterThan(joinEvents.count, 0)

    // Check that the presence enabled flag is set to true in the join payload
    if let joinEvent = joinEvents.first,
      let config = joinEvent.payload["config"]?.objectValue,
      let presence = config["presence"]?.objectValue,
      let enabled = presence["enabled"]?.boolValue
    {
      XCTAssertTrue(enabled, "Presence should be enabled when presence callback exists")
    } else {
      XCTFail("Could not find presence enabled flag in join payload")
    }

    // Clean up
    presenceSubscription.cancel()
    await channel.unsubscribe()
    socket.disconnect()

    // Note: We don't assert the subscribe status here because the test doesn't wait for completion
    // The subscription is still in progress when we clean up
  }

  func testHttpSendThrowsWhenAccessTokenIsMissing() async {
    let httpClient = HTTPClientMock()
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(headers: ["apikey": "test-key"]),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    do {
      try await channel.httpSend(event: "test", message: ["data": "test"])
      XCTFail("Expected httpSend to throw an error when access token is missing")
    } catch {
      XCTAssertEqual(error.localizedDescription, "Access token is required for httpSend()")
    }
  }

  func testHttpSendSucceedsOn202Status() async throws {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 202,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic") { config in
      config.isPrivate = true
    }

    try await channel.httpSend(event: "test-event", message: ["data": "explicit"])

    let requests = await httpClient.receivedRequests
    XCTAssertEqual(requests.count, 1)

    let request = requests[0]
    XCTAssertEqual(
      request.url.absoluteString,
      "https://localhost:54321/realtime/v1/api/broadcast/test-topic/events/test-event?private=true"
    )
    XCTAssertEqual(request.method, .post)
    XCTAssertEqual(request.headers[.authorization], "Bearer test-token")
    XCTAssertEqual(request.headers[.apiKey], "test-key")
    XCTAssertEqual(request.headers[.contentType], "application/json")

    let body = try JSONDecoder().decode([String: String].self, from: request.body ?? Data())
    XCTAssertEqual(body, ["data": "explicit"])
  }

  func testHttpSendPercentEncodesTopicAndEventInURL() async throws {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 202,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("room/one")

    try await channel.httpSend(event: "cursor move", message: ["x": 1])

    let requests = await httpClient.receivedRequests
    XCTAssertEqual(
      requests[0].url.absoluteString,
      "https://localhost:54321/realtime/v1/api/broadcast/room%2Fone/events/cursor%20move"
    )
  }

  func testHttpSendWithBinaryDataSendsOctetStream() async throws {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 202,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    let payload = Data([0x01, 0x02, 0x03])
    try await channel.httpSend(event: "binary-event", data: payload)

    let requests = await httpClient.receivedRequests
    XCTAssertEqual(requests.count, 1)

    let request = requests[0]
    XCTAssertEqual(
      request.url.absoluteString,
      "https://localhost:54321/realtime/v1/api/broadcast/test-topic/events/binary-event"
    )
    XCTAssertEqual(request.headers[.contentType], "application/octet-stream")
    XCTAssertEqual(request.body, payload)
  }

  func testHttpSendWithBinaryDataThrowsWhenAccessTokenIsMissing() async {
    let httpClient = HTTPClientMock()
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(headers: ["apikey": "test-key"]),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    do {
      try await channel.httpSend(event: "test", data: Data([0x01]))
      XCTFail("Expected httpSend to throw an error when access token is missing")
    } catch {
      XCTAssertEqual(error.localizedDescription, "Access token is required for httpSend()")
    }
  }

  func testHttpSendThrowsOnNon202Status() async {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      let errorBody = try JSONEncoder().encode(["error": "Server error"])
      return HTTPResponse(
        data: errorBody,
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 500,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    do {
      try await channel.httpSend(event: "test", message: ["data": "test"])
      XCTFail("Expected httpSend to throw an error on non-202 status")
    } catch {
      XCTAssertEqual(error.localizedDescription, "Server error")
    }
  }

  func testHttpSendRespectsCustomTimeout() async throws {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 202,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        timeoutInterval: 5.0,
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    // Test with custom timeout
    try await channel.httpSend(event: "test", message: ["data": "test"], timeout: 3.0)

    let requests = await httpClient.receivedRequests
    XCTAssertEqual(requests.count, 1)
  }

  func testHttpSendUsesDefaultTimeoutWhenNotSpecified() async throws {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 202,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        timeoutInterval: 5.0,
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    // Test without custom timeout
    try await channel.httpSend(event: "test", message: ["data": "test"])

    let requests = await httpClient.receivedRequests
    XCTAssertEqual(requests.count, 1)
  }

  func testHttpSendFallsBackToStatusTextWhenErrorBodyHasNoErrorField() async {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      let errorBody = try JSONEncoder().encode(["message": "Invalid request"])
      return HTTPResponse(
        data: errorBody,
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 400,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    do {
      try await channel.httpSend(event: "test", message: ["data": "test"])
      XCTFail("Expected httpSend to throw an error on 400 status")
    } catch {
      XCTAssertEqual(error.localizedDescription, "Invalid request")
    }
  }

  func testHttpSendFallsBackToStatusTextWhenJSONParsingFails() async {
    let httpClient = HTTPClientMock()
    await httpClient.when({ _ in true }) { _ in
      HTTPResponse(
        data: Data("Invalid JSON".utf8),
        response: HTTPURLResponse(
          url: URL(string: "https://localhost:54321/api/broadcast")!,
          statusCode: 503,
          httpVersion: nil,
          headerFields: nil
        )!
      )
    }
    let (client, _) = FakeWebSocket.fakes()

    let socket = RealtimeClientV2(
      url: URL(string: "https://localhost:54321/realtime/v1")!,
      options: RealtimeClientOptions(
        headers: ["apikey": "test-key"],
        accessToken: { "test-token" }
      ),
      wsTransport: { _, _ in client },
      http: httpClient
    )

    let channel = socket.channel("test-topic")

    do {
      try await channel.httpSend(event: "test", message: ["data": "test"])
      XCTFail("Expected httpSend to throw an error on 503 status")
    } catch {
      // Should fall back to localized status text (case-insensitive)
      let description = error.localizedDescription.lowercased()
      XCTAssertTrue(
        description.contains("503") || description.contains("unavailable"),
        "Expected status text fallback, got '\(error.localizedDescription)'"
      )
    }
  }
}

extension RealtimeChannelTests {
  @MainActor
  private func waitForChannelStatus(
    _ status: RealtimeChannelStatus,
    channel: RealtimeChannelV2,
    timeout: TimeInterval,
    pollInterval: UInt64 = 10_000_000
  ) async {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
      if channel.status == status { return }
      try? await Task.sleep(nanoseconds: pollInterval)
    }
  }

  @MainActor
  private func waitForEvents(
    in socket: FakeWebSocket,
    event: String,
    timeout: TimeInterval,
    pollInterval: UInt64 = 10_000_000
  ) async -> [RealtimeMessageV2] {
    let deadline = Date().addingTimeInterval(timeout)

    while Date() < deadline {
      let events = socket.receivedEvents.compactMap { $0.realtimeMessage }.filter {
        $0.event == event
      }

      if !events.isEmpty {
        return events
      }

      try? await Task.sleep(nanoseconds: pollInterval)
    }

    return []
  }

  @MainActor
  private func waitForSentEvents(
    in socket: FakeWebSocket,
    event: String,
    timeout: TimeInterval,
    pollInterval: UInt64 = 10_000_000
  ) async -> [RealtimeMessageV2] {
    let deadline = Date().addingTimeInterval(timeout)

    while Date() < deadline {
      let events = socket.sentEvents.compactMap(\.realtimeMessage).filter {
        $0.event == event
      }

      if !events.isEmpty {
        return events
      }

      try? await Task.sleep(nanoseconds: pollInterval)
    }

    return []
  }
}

private actor TokenGate {
  private var requestCount = 0
  private var continuations: [CheckedContinuation<String?, Never>] = []

  func token() async -> String? {
    requestCount += 1
    return await withCheckedContinuation { continuation in
      continuations.append(continuation)
    }
  }

  func waitForRequestCount(_ count: Int, timeout: TimeInterval = 1) async -> Bool {
    let deadline = Date().addingTimeInterval(timeout)
    while requestCount < count && Date() < deadline {
      try? await Task.sleep(nanoseconds: 5_000_000)
    }
    return requestCount >= count
  }

  func resumeNext(with token: String?) {
    guard !continuations.isEmpty else { return }
    continuations.removeFirst().resume(returning: token)
  }

  func resumeAll(with token: String?) {
    let pending = continuations
    continuations.removeAll()
    for continuation in pending {
      continuation.resume(returning: token)
    }
  }
}
