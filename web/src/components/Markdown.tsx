import ReactMarkdown from 'react-markdown';

export default function Markdown({ children }: { children: string }) {
  return (
    <div className="text-sm text-slate-700 leading-relaxed break-words">
      <ReactMarkdown
        components={{
          a: ({ href, children }) => (
            <a
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-700 underline break-all"
            >
              {children}
            </a>
          ),
          p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
          h1: ({ children }) => <h1 className="text-lg font-bold text-slate-900 mt-4 mb-2">{children}</h1>,
          h2: ({ children }) => <h2 className="text-base font-semibold text-slate-900 mt-3 mb-2">{children}</h2>,
          h3: ({ children }) => <h3 className="text-sm font-semibold text-slate-900 mt-3 mb-1">{children}</h3>,
          ul: ({ children }) => <ul className="list-disc pl-5 mb-2 space-y-0.5">{children}</ul>,
          ol: ({ children }) => <ol className="list-decimal pl-5 mb-2 space-y-0.5">{children}</ol>,
          li: ({ children }) => <li className="text-sm text-slate-700">{children}</li>,
          blockquote: ({ children }) => (
            <blockquote className="border-l-2 border-slate-300 pl-3 my-2 text-slate-500 italic">{children}</blockquote>
          ),
          code: ({ children }) => (
            <code className="bg-slate-100 text-slate-800 px-1 py-0.5 rounded text-xs">{children}</code>
          ),
          hr: () => <hr className="my-3 border-slate-200" />,
        }}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
}
