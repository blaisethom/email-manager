import { useEffect, useRef, useState } from 'react';

interface LogViewerProps {
  jobId: number;
  jobStatus: string;
}

interface LogLine {
  text: string;
  stream?: 'stdout' | 'stderr';
}

export default function LogViewer({ jobId, jobStatus }: LogViewerProps) {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [connected, setConnected] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);

  // Track if user has scrolled up (disable auto-scroll)
  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    autoScrollRef.current = atBottom;
  };

  // Auto-scroll to bottom when new lines arrive
  useEffect(() => {
    if (autoScrollRef.current && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines]);

  // SSE connection
  useEffect(() => {
    const eventSource = new EventSource(`/api/jobs/${jobId}/logs`);
    setConnected(true);

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'log') {
          setLines((prev) => {
            const next = [...prev, { text: data.line, stream: data.stream }];
            // Keep last 5000 lines in the UI
            return next.length > 5000 ? next.slice(-5000) : next;
          });
        } else if (data.type === 'status') {
          // Job finished — SSE will close
        }
      } catch {
        // ignore parse errors
      }
    };

    eventSource.onerror = () => {
      setConnected(false);
      eventSource.close();
    };

    return () => {
      eventSource.close();
      setConnected(false);
    };
  }, [jobId]);

  const isTerminal = jobStatus === 'completed' || jobStatus === 'failed' || jobStatus === 'cancelled';

  return (
    <div className="flex flex-col h-full">
      {/* Status bar */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-slate-800 border-b border-slate-700 text-xs">
        <span className="text-slate-400">
          {lines.length} lines
        </span>
        <span className={connected && !isTerminal ? 'text-green-400' : 'text-slate-500'}>
          {isTerminal ? 'finished' : connected ? 'streaming' : 'disconnected'}
        </span>
      </div>

      {/* Log content */}
      <div
        ref={containerRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto p-3 bg-slate-900 font-mono text-xs leading-5 select-text"
      >
        {lines.length === 0 && (
          <div className="text-slate-500 italic">
            {jobStatus === 'queued' ? 'Waiting to start...' : 'No output yet...'}
          </div>
        )}
        {lines.map((line, i) => (
          <div key={i} className={lineClassName(line)}>
            {line.text}
          </div>
        ))}
      </div>
    </div>
  );
}

function lineClassName(line: LogLine): string {
  const text = line.text;

  if (line.stream === 'stderr') {
    return 'text-red-400';
  }
  // Error lines
  if (/\bfailed\b|\bFAIL\b|\bERROR\b|\bTraceback\b/i.test(text)) {
    return 'text-red-400';
  }
  // Stage results (green)
  if (/\b(generated|created|updated|labelled|analysed|proposed)\b/i.test(text)) {
    return 'text-green-400';
  }
  // Stage headers
  if (/^Running stage:|^={2,}|Company \d+\/\d+:/i.test(text)) {
    return 'text-blue-300 font-semibold';
  }
  // Skip/dim lines
  if (/skipped|up to date|nothing to do/i.test(text)) {
    return 'text-slate-500';
  }
  // Progress lines
  if (/\d+\/\d+\s+(threads|companies|discussions)/.test(text)) {
    return 'text-cyan-300';
  }
  // AI backend info and prompt/trace section content
  if (/Using AI backend:|^\[Global rules|^\[Company-specific instructions/i.test(text)) {
    return 'text-yellow-300';
  }

  return 'text-slate-300';
}
