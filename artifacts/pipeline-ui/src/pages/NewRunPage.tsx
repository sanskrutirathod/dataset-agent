import { useState } from "react";
import { useLocation } from "wouter";
import { useMutation } from "@tanstack/react-query";
import { startPipelineRun } from "@/lib/pipeline-api";
import type { PipelineConfig, SourceConfig } from "@/lib/pipeline-api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Plus, Trash2, Zap, ArrowLeft } from "lucide-react";
import { Link } from "wouter";

interface SourceEntry {
  id: string;
  type: "url" | "text";
  value: string;
  title: string;
}

function genId() {
  return Math.random().toString(36).slice(2, 8);
}

export default function NewRunPage() {
  const [, navigate] = useLocation();

  const [runName, setRunName] = useState("");
  const [sources, setSources] = useState<SourceEntry[]>([
    { id: genId(), type: "text", value: "", title: "" },
  ]);
  const [genMode, setGenMode] = useState<"qa" | "instruction" | "summary" | "chat">("qa");
  const [maxRecords, setMaxRecords] = useState(100);
  const [maxPerChunk, setMaxPerChunk] = useState(3);
  const [targetTokens, setTargetTokens] = useState(200);
  const [scoreThreshold, setScoreThreshold] = useState(0.3);
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: (config: PipelineConfig) => startPipelineRun(config),
    onSuccess: (data) => {
      navigate(`/runs/${data.run_id}`);
    },
    onError: (err: Error) => {
      setError(err.message);
    },
  });

  const addSource = () => {
    setSources((prev) => [...prev, { id: genId(), type: "text", value: "", title: "" }]);
  };

  const removeSource = (id: string) => {
    setSources((prev) => prev.filter((s) => s.id !== id));
  };

  const updateSource = (id: string, patch: Partial<SourceEntry>) => {
    setSources((prev) => prev.map((s) => (s.id === id ? { ...s, ...patch } : s)));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!runName.trim()) {
      setError("Run name is required.");
      return;
    }
    const validSources = sources.filter((s) => s.value.trim());
    if (validSources.length === 0) {
      setError("At least one source is required.");
      return;
    }

    const config: PipelineConfig = {
      run_name: runName.trim(),
      sources: validSources.map((s) => ({
        type: s.type,
        value: s.value.trim(),
        title: s.title.trim() || undefined,
      } as SourceConfig)),
      chunk: { target_tokens: targetTokens, overlap: Math.round(targetTokens * 0.2) },
      generation: { mode: genMode, max_records_per_chunk: maxPerChunk },
      validation: { min_length: 20, score_threshold: scoreThreshold },
      limits: { max_records: maxRecords, max_per_source: Math.ceil(maxRecords / validSources.length) },
    };

    mutation.mutate(config);
  };

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <Link href="/">
          <Button variant="ghost" size="sm" className="gap-1 text-gray-500">
            <ArrowLeft className="w-4 h-4" />
            Runs
          </Button>
        </Link>
        <div className="h-4 w-px bg-gray-300" />
        <h1 className="text-xl font-bold text-gray-900">New Pipeline Run</h1>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="bg-white rounded-xl border border-gray-200 p-5 space-y-4">
          <h2 className="text-sm font-semibold text-gray-700">Run Settings</h2>
          <div>
            <Label htmlFor="runName" className="text-sm">Run Name</Label>
            <Input
              id="runName"
              value={runName}
              onChange={(e) => setRunName(e.target.value)}
              placeholder="e.g., ml-qa-v1"
              className="mt-1"
            />
          </div>
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-5 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold text-gray-700">Sources</h2>
            <Button type="button" variant="outline" size="sm" onClick={addSource} className="gap-1">
              <Plus className="w-3 h-3" />
              Add Source
            </Button>
          </div>

          {sources.map((source, idx) => (
            <div key={source.id} className="border border-gray-100 rounded-lg p-4 space-y-3 bg-gray-50">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-500">Source {idx + 1}</span>
                {sources.length > 1 && (
                  <button
                    type="button"
                    onClick={() => removeSource(source.id)}
                    className="text-gray-400 hover:text-red-500 transition-colors"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                )}
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Label className="text-xs">Type</Label>
                  <Select
                    value={source.type}
                    onValueChange={(v) => updateSource(source.id, { type: v as "url" | "text" })}
                  >
                    <SelectTrigger className="mt-1 h-9">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="text">Text</SelectItem>
                      <SelectItem value="url">URL</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label className="text-xs">Title (optional)</Label>
                  <Input
                    value={source.title}
                    onChange={(e) => updateSource(source.id, { title: e.target.value })}
                    placeholder="Descriptive name"
                    className="mt-1 h-9"
                  />
                </div>
              </div>
              {source.type === "text" ? (
                <div>
                  <Label className="text-xs">Text Content</Label>
                  <Textarea
                    value={source.value}
                    onChange={(e) => updateSource(source.id, { value: e.target.value })}
                    placeholder="Paste text to generate training data from..."
                    className="mt-1 min-h-24 text-sm"
                  />
                </div>
              ) : (
                <div>
                  <Label className="text-xs">URL</Label>
                  <Input
                    value={source.value}
                    onChange={(e) => updateSource(source.id, { value: e.target.value })}
                    placeholder="https://example.com/article"
                    className="mt-1"
                  />
                </div>
              )}
            </div>
          ))}
        </div>

        <div className="bg-white rounded-xl border border-gray-200 p-5 space-y-5">
          <h2 className="text-sm font-semibold text-gray-700">Generation Settings</h2>

          <div>
            <Label className="text-sm">Generation Mode</Label>
            <Select value={genMode} onValueChange={(v) => setGenMode(v as typeof genMode)}>
              <SelectTrigger className="mt-1">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="qa">Q&A Pairs</SelectItem>
                <SelectItem value="instruction">Instruction Following</SelectItem>
                <SelectItem value="summary">Summarization</SelectItem>
                <SelectItem value="chat">Chat / Dialogue</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div>
            <Label className="text-sm">Max Total Records: {maxRecords}</Label>
            <Slider
              min={10}
              max={1000}
              step={10}
              value={[maxRecords]}
              onValueChange={([v]) => setMaxRecords(v)}
              className="mt-2"
            />
          </div>

          <div>
            <Label className="text-sm">Records per Chunk: {maxPerChunk}</Label>
            <Slider
              min={1}
              max={10}
              step={1}
              value={[maxPerChunk]}
              onValueChange={([v]) => setMaxPerChunk(v)}
              className="mt-2"
            />
          </div>

          <div>
            <Label className="text-sm">Chunk Size (tokens): {targetTokens}</Label>
            <Slider
              min={50}
              max={1000}
              step={50}
              value={[targetTokens]}
              onValueChange={([v]) => setTargetTokens(v)}
              className="mt-2"
            />
          </div>

          <div>
            <Label className="text-sm">Min Quality Score: {(scoreThreshold * 100).toFixed(0)}%</Label>
            <Slider
              min={0}
              max={1}
              step={0.05}
              value={[scoreThreshold]}
              onValueChange={([v]) => setScoreThreshold(v)}
              className="mt-2"
            />
          </div>
        </div>

        {error && (
          <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-4 py-3">
            {error}
          </div>
        )}

        <Button
          type="submit"
          className="w-full gap-2 bg-indigo-600 hover:bg-indigo-700"
          disabled={mutation.isPending}
        >
          <Zap className="w-4 h-4" />
          {mutation.isPending ? "Starting Pipeline..." : "Start Pipeline Run"}
        </Button>
      </form>
    </div>
  );
}
