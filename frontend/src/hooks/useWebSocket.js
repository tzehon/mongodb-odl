import { useState, useEffect, useCallback, useRef } from 'react';

const WS_BASE = import.meta.env.VITE_WS_URL || `ws://${window.location.host}`;

/**
 * Custom hook for WebSocket connections to change streams
 */
export function useWebSocket(accountNumber = null) {
  const [isConnected, setIsConnected] = useState(false);
  const [events, setEvents] = useState([]);
  const [error, setError] = useState(null);
  const [connectionId, setConnectionId] = useState(null);
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const maxEvents = 100;

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    const endpoint = accountNumber
      ? `${WS_BASE}/ws/changes/${accountNumber}`
      : `${WS_BASE}/ws/changes`;

    try {
      const ws = new WebSocket(endpoint);

      ws.onopen = () => {
        setIsConnected(true);
        setError(null);
        console.log('WebSocket connected');
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          if (data.type === 'connected') {
            setConnectionId(data.connectionId);
            return;
          }

          if (data.type === 'error') {
            setError(data.message);
            return;
          }

          // Add event to the list
          setEvents((prev) => {
            const newEvents = [{ ...data, id: Date.now() }, ...prev];
            return newEvents.slice(0, maxEvents);
          });
        } catch (e) {
          console.error('Failed to parse WebSocket message:', e);
        }
      };

      ws.onerror = (event) => {
        console.error('WebSocket error:', event);
        setError('Connection error');
      };

      ws.onclose = (event) => {
        setIsConnected(false);
        setConnectionId(null);
        console.log('WebSocket closed:', event.code, event.reason);

        // Attempt reconnection after 3 seconds
        if (!event.wasClean) {
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log('Attempting to reconnect...');
            connect();
          }, 3000);
        }
      };

      wsRef.current = ws;
    } catch (e) {
      console.error('Failed to create WebSocket:', e);
      setError(e.message);
    }
  }, [accountNumber]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    if (wsRef.current) {
      wsRef.current.close(1000, 'Client disconnect');
      wsRef.current = null;
    }
    setIsConnected(false);
    setConnectionId(null);
  }, []);

  const clearEvents = useCallback(() => {
    setEvents([]);
  }, []);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close(1000, 'Component unmount');
      }
    };
  }, [connect]);

  return {
    isConnected,
    events,
    error,
    connectionId,
    connect,
    disconnect,
    clearEvents,
  };
}

export default useWebSocket;
