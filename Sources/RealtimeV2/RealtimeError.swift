//
//  RealtimeError.swift
//
//
//  Created by Guilherme Souza on 30/10/23.
//

public import Foundation
import Helpers

package struct RealtimeError: LocalizedError {
  package var errorDescription: String?

  package init(_ errorDescription: String) {
    self.errorDescription = errorDescription
  }
}

extension RealtimeError {
  /// The maximum retry attempts reached.
  static var maxRetryAttemptsReached: Self {
    Self("Maximum retry attempts reached.")
  }
}

/// Errors surfaced by ``RealtimeChannelV2/subscribeWithError()``.
public enum RealtimeSubscribeError: Error, Equatable, LocalizedError, Sendable {
  /// The connection failed for a known transient network condition.
  ///
  /// This case is limited to connectivity failures that may recover without
  /// changing channel authorization or configuration. It does not include
  /// timeouts, authentication or handshake failures, rejected joins, or
  /// unknown transport errors.
  case transientTransportFailure

  /// The server rejected the correlated channel join.
  ///
  /// The server's rejection details are intentionally not exposed by this
  /// error. Resolve any authorization or configuration issue before retrying.
  case joinRejected

  /// The connection failed for a terminal or unclassified reason.
  ///
  /// The underlying transport error is intentionally not exposed because it
  /// may contain connection URLs, credentials, or other private details.
  case connectionFailed

  public var errorDescription: String? {
    switch self {
    case .transientTransportFailure:
      "Realtime transport is temporarily unavailable."
    case .joinRejected:
      "The Realtime server rejected the channel subscription."
    case .connectionFailed:
      "The Realtime connection failed."
    }
  }

  static func sanitizedConnectionFailure(for error: any Error) -> any Error {
    if let transientError = transientTransportFailure(for: error) {
      return transientError
    }
    if error is CancellationError {
      return CancellationError()
    }
    if error is TimeoutError {
      return error
    }

    let underlyingError: any Error
    if let webSocketError = error as? WebSocketError,
      case .connection(_, let error) = webSocketError
    {
      underlyingError = error
    } else {
      underlyingError = error
    }

    if underlyingError is CancellationError {
      return CancellationError()
    }
    if underlyingError is TimeoutError {
      return underlyingError
    }
    if let urlError = underlyingError as? URLError {
      switch urlError.code {
      case .timedOut:
        return URLError(.timedOut)
      case .cancelled:
        return CancellationError()
      default:
        return RealtimeSubscribeError.connectionFailed
      }
    }

    return RealtimeSubscribeError.connectionFailed
  }

  static func transientTransportFailure(for error: any Error) -> Self? {
    let underlyingError: any Error
    if let webSocketError = error as? WebSocketError,
      case .connection(_, let error) = webSocketError
    {
      underlyingError = error
    } else {
      underlyingError = error
    }

    guard let urlError = underlyingError as? URLError else {
      return nil
    }

    switch urlError.code {
    case .networkConnectionLost, .notConnectedToInternet:
      return .transientTransportFailure
    default:
      return nil
    }
  }
}
