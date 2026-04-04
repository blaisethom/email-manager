type Variant = 'label' | 'state' | 'category';

interface BadgeProps {
  label: string;
  variant?: Variant;
}

const STATE_COLORS: Record<string, string> = {
  active: 'bg-green-100 text-green-800',
  open: 'bg-green-100 text-green-800',
  ongoing: 'bg-green-100 text-green-800',
  in_progress: 'bg-green-100 text-green-800',
  completed: 'bg-slate-100 text-slate-700',
  closed: 'bg-slate-100 text-slate-700',
  done: 'bg-slate-100 text-slate-700',
  resolved: 'bg-slate-100 text-slate-700',
  stalled: 'bg-amber-100 text-amber-800',
  blocked: 'bg-amber-100 text-amber-800',
  pending: 'bg-amber-100 text-amber-800',
  new: 'bg-blue-100 text-blue-800',
  initial: 'bg-blue-100 text-blue-800',
  proposed: 'bg-blue-100 text-blue-800',
  draft: 'bg-blue-100 text-blue-800',
};

const HASH_COLORS = [
  'bg-blue-100 text-blue-800',
  'bg-green-100 text-green-800',
  'bg-purple-100 text-purple-800',
  'bg-orange-100 text-orange-800',
  'bg-rose-100 text-rose-800',
  'bg-amber-100 text-amber-800',
  'bg-teal-100 text-teal-800',
  'bg-cyan-100 text-cyan-800',
];

function stringHash(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function getHashColor(str: string): string {
  return HASH_COLORS[stringHash(str) % HASH_COLORS.length];
}

export default function Badge({ label, variant = 'label' }: BadgeProps) {
  let colorClass: string;

  if (variant === 'state') {
    const key = label.toLowerCase().replace(/[\s-]/g, '_');
    colorClass = STATE_COLORS[key] ?? 'bg-slate-100 text-slate-700';
  } else {
    colorClass = getHashColor(label);
  }

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${colorClass}`}>
      {label}
    </span>
  );
}
