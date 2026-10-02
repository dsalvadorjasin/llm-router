export interface Conversation {
  id: string;
  title: string;
  created_at: number;
  updated_at: number;
  pinned: boolean;
}

export interface Usage {
  prompt_tokens: number;
  completion_tokens: number;
}

export interface Message {
  id: string;
  conversation_id: string;
  role: "user" | "assistant";
  content: string;
  latency_ms: number | null;
  created_at: number;
  // Only ever set client-side, on the message just returned by /v1/chat —
  // the backend doesn't persist usage per message, so it's unavailable
  // again once a conversation is reloaded from history.
  usage?: Usage | null;
}

export interface ChatResponse {
  message: Message;
  latency_ms: number;
  model: string | null;
  usage: Usage | null;
  upstream?: string;
}
