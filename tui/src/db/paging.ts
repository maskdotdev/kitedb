export interface PageMeta {
  cursor?: string;
  nextCursor?: string;
  hasMore: boolean;
}

export interface CursorMove {
  cursor?: string;
  history: string[];
}

export function nextPage(meta: PageMeta, history: string[]): CursorMove {
  if (!meta.hasMore || !meta.nextCursor) {
    return { cursor: meta.cursor, history };
  }
  return {
    cursor: meta.nextCursor,
    history: [...history, meta.cursor ?? ""],
  };
}

export function prevPage(history: string[]): CursorMove {
  if (history.length === 0) {
    return { cursor: undefined, history };
  }
  const nextHistory = history.slice(0, -1);
  const cursor = history[history.length - 1];
  return { cursor: cursor || undefined, history: nextHistory };
}
