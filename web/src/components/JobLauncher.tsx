import { useState, useEffect } from 'react';
import { api } from '../api';
import type { PipelineStage, JobConfig } from '../types';

interface JobLauncherProps {
  open: boolean;
  onClose: () => void;
  onCreated: (jobId: number) => void;
  defaultCompany?: string;
}

export default function JobLauncher({ open, onClose, onCreated, defaultCompany }: JobLauncherProps) {
  const [stages, setStages] = useState<PipelineStage[]>([]);
  const [labels, setLabels] = useState<string[]>([]);

  // Form state
  const [jobType, setJobType] = useState<'sync' | 'analyse'>('analyse');
  const [selectedStages, setSelectedStages] = useState<Set<string>>(new Set());
  const [company, setCompany] = useState(defaultCompany ?? '');
  const [label, setLabel] = useState('');
  const [force, setForce] = useState(false);
  const [clean, setClean] = useState(false);
  const [perCompany, setPerCompany] = useState(false);
  const [concurrency, setConcurrency] = useState(1);
  const [newEmails, setNewEmails] = useState(false);
  const [staleModel, setStaleModel] = useState(false);
  const [stalePrompt, setStalePrompt] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load stages and labels
  useEffect(() => {
    if (!open) return;
    api.getStages().then(({ stages }) => {
      setStages(stages);
      // Default: select all stages
      setSelectedStages(new Set(stages.map(s => s.name)));
    }).catch(console.error);

    api.getMeta().then(meta => setLabels(meta.labels)).catch(console.error);
  }, [open]);

  // Reset form when opening with a default company
  useEffect(() => {
    if (open && defaultCompany) {
      setCompany(defaultCompany);
      setJobType('analyse');
    }
  }, [open, defaultCompany]);

  if (!open) return null;

  const toggleStage = (name: string) => {
    setSelectedStages(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const selectAllStages = () => setSelectedStages(new Set(stages.map(s => s.name)));
  const selectNone = () => setSelectedStages(new Set());

  const handleSubmit = async () => {
    setError(null);
    setSubmitting(true);

    try {
      const config: JobConfig = { job_type: jobType };

      if (jobType === 'analyse') {
        const stageNames = [...selectedStages];
        // Only set stages if not all selected
        if (stageNames.length < stages.length) {
          config.stages = stageNames;
        }
        if (company.trim()) config.company = company.trim();
        if (label) config.label = label;
        if (force) config.force = true;
        if (clean) config.clean = true;
        if (perCompany) config.per_company = true;
        if (concurrency > 1) config.concurrency = concurrency;
        if (newEmails) config.new_emails = true;
        if (staleModel) config.stale_model = true;
        if (stalePrompt) config.stale_prompt = true;
      }

      const job = await api.createJob(config);
      onCreated(job.id);
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to create job');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-[85vh] overflow-y-auto mx-4"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold text-slate-900">New Pipeline Job</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-600 text-xl leading-none">&times;</button>
        </div>

        <div className="px-6 py-4 space-y-4">
          {/* Job Type */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Job Type</label>
            <div className="flex gap-2">
              <button
                className={`px-4 py-2 text-sm rounded-lg border ${jobType === 'sync' ? 'bg-blue-50 border-blue-300 text-blue-700' : 'border-slate-300 text-slate-700 hover:bg-slate-50'}`}
                onClick={() => setJobType('sync')}
              >
                Sync Emails
              </button>
              <button
                className={`px-4 py-2 text-sm rounded-lg border ${jobType === 'analyse' ? 'bg-blue-50 border-blue-300 text-blue-700' : 'border-slate-300 text-slate-700 hover:bg-slate-50'}`}
                onClick={() => setJobType('analyse')}
              >
                Analyse
              </button>
            </div>
          </div>

          {jobType === 'analyse' && (
            <>
              {/* Stages */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="block text-sm font-medium text-slate-700">Stages</label>
                  <div className="flex gap-2 text-xs">
                    <button onClick={selectAllStages} className="text-blue-600 hover:underline">All</button>
                    <button onClick={selectNone} className="text-blue-600 hover:underline">None</button>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-1">
                  {stages.map(stage => (
                    <label key={stage.name} className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={selectedStages.has(stage.name)}
                        onChange={() => toggleStage(stage.name)}
                        className="rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-slate-700">{stage.name.replace(/_/g, ' ')}</span>
                      {stage.needs_ai && <span className="text-xs text-amber-600">AI</span>}
                    </label>
                  ))}
                </div>
              </div>

              {/* Filters */}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Company</label>
                  <input
                    type="text"
                    value={company}
                    onChange={e => setCompany(e.target.value)}
                    placeholder="e.g. acme.com"
                    className="filter-input w-full"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Label</label>
                  <select
                    value={label}
                    onChange={e => setLabel(e.target.value)}
                    className="filter-input w-full"
                  >
                    <option value="">All companies</option>
                    {labels.map(l => <option key={l} value={l}>{l}</option>)}
                  </select>
                </div>
              </div>

              {/* Options */}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Options</label>
                <div className="grid grid-cols-2 gap-1">
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>Force re-run</span>
                  </label>
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={clean} onChange={e => setClean(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>Clean first</span>
                  </label>
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={perCompany} onChange={e => setPerCompany(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>Per-company mode</span>
                  </label>
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={newEmails} onChange={e => setNewEmails(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>New emails only</span>
                  </label>
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={staleModel} onChange={e => setStaleModel(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>Stale model only</span>
                  </label>
                  <label className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-slate-50 cursor-pointer">
                    <input type="checkbox" checked={stalePrompt} onChange={e => setStalePrompt(e.target.checked)} className="rounded border-slate-300 text-blue-600 focus:ring-blue-500" />
                    <span>Stale prompt only</span>
                  </label>
                </div>
              </div>

              {/* Concurrency */}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Concurrency</label>
                <input
                  type="number"
                  min={1}
                  max={10}
                  value={concurrency}
                  onChange={e => setConcurrency(Math.max(1, parseInt(e.target.value) || 1))}
                  className="filter-input w-20"
                />
                <span className="text-xs text-slate-500 ml-2">Parallel LLM calls per stage</span>
              </div>
            </>
          )}

          {/* Error */}
          {error && (
            <div className="text-sm text-red-600 bg-red-50 px-3 py-2 rounded-lg">{error}</div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-slate-200">
          <button onClick={onClose} className="btn-secondary">Cancel</button>
          <button
            onClick={handleSubmit}
            disabled={submitting || (jobType === 'analyse' && selectedStages.size === 0)}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {submitting ? 'Creating...' : 'Launch Job'}
          </button>
        </div>
      </div>
    </div>
  );
}
