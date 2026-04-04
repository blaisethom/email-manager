interface EmptyStateProps {
  title?: string;
  message?: string;
  icon?: React.ReactNode;
}

function DefaultIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="40"
      height="40"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className="text-slate-300"
    >
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.35-4.35" />
    </svg>
  );
}

export default function EmptyState({
  title = 'No results found',
  message = 'Try adjusting your search or filters.',
  icon,
}: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <div className="mb-4">{icon ?? <DefaultIcon />}</div>
      <h3 className="text-base font-medium text-slate-700 mb-1">{title}</h3>
      <p className="text-sm text-slate-500 max-w-xs">{message}</p>
    </div>
  );
}
