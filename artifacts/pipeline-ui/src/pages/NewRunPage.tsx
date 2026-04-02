import { useState } from "react";
import { useLocation } from "wouter";
import { useMutation } from "@tanstack/react-query";
import { startPipelineRun } from "@/lib/pipeline-api";
import type { PipelineConfig, AnySourceConfig } from "@/lib/pipeline-api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue, SelectGroup, SelectLabel } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Plus, Trash2, Zap, ArrowLeft, ChevronDown, ChevronUp } from "lucide-react";
import { Link } from "wouter";

type SourceType = "url" | "text" | "crawl";

interface SourceEntry {
  id: string;
  type: SourceType;
  value: string;
  title: string;
  crawlMaxDepth: number;
  crawlMaxPages: number;
  crawlAllowedDomains: string;
  crawlDelayMs: number;
  crawlSettingsOpen: boolean;
}

type StandardGenMode = "qa" | "instruction" | "summary" | "chat";
type DistillationGenMode = "distillation_cot" | "distillation_dpo" | "distillation_sft";
type GenMode = StandardGenMode | DistillationGenMode;

const DISTILLATION_MODES: DistillationGenMode[] = ["distillation_cot", "distillation_dpo", "distillation_sft"];

function isDistillationMode(mode: GenMode): mode is DistillationGenMode {
  return DISTILLATION_MODES.includes(mode as DistillationGenMode);
}

function genId() {
  return Math.random().toString(36).slice(2, 8);
}

function defaultSource(): SourceEntry {
  return {
    id: genId(),
    type: "text",
    value: "",
    title: "",
    crawlMaxDepth: 2,
    crawlMaxPages: 50,
    crawlAllowedDomains: "",
    crawlDelayMs: 500,
    crawlSettingsOpen: false,
  };
}

export default function NewRunPage() {
  const [, navigate] = useLocation();

  const [runName, setRunName] = useState("");
  const [sources, setSources] = useState<SourceEntry[]>([defaultSource()]);
  const [genMode, setGenMode] = useState<GenMode>("qa");
  const [teacherModel, setTeacherModel] = useState("gpt-5-mini");
  const [maxRecords, setMaxRecords] = useState(100);
  const [maxPerChunk, setMaxPerChunk] = useState(3);
  const [targetTokens, setTargetTokens] = useState(200);
  const [chunkOverlap, setChunkOverlap] = useState(40);
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
    setSources((prev) => [...prev, defaultSource()]);
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

    const validSources = sources.filter((s) => {
      if (s.type === "crawl") return s.value.trim().length > 0;
      return s.value.trim().length > 0;
    });

    if (validSources.length === 0) {
      setError("At least one source with content is required.");
      return;
    }

    const mappedSources: AnySourceConfig[] = validSources.map((s) => {
      if (s.type === "crawl") {
        const allowedDomains = s.crawlAllowedDomains
          .split(",")
          .map((d) => d.trim())
          .filter(Boolean);
        return {
          type: "crawl" as const,
          seed_url: s.value.trim(),
          max_depth: s.crawlMaxDepth,
          max_pages: s.crawlMaxPages,
          allowed_domains: allowedDomains,
          delay_ms: s.crawlDelayMs,
        };
      }
      return {
        type: s.type as "url" | "text",
        value: s.value.trim(),
        title: s.title.trim() || undefined,
      };
    });

    const distillMode = isDistillationMode(genMode) ? genMode.replace("distillation_", "") : undefined;

    const generation: PipelineConfig["generation"] = {
      mode: isDistillationMode(genMode) ? "qa" : (genMode as StandardGenMode),
      max_records_per_chunk: maxPerChunk,
    };

    if (distillMode) {
      generation.distillation_mode = distillMode as "cot" | "dpo" | "sft";
      generation.teacher_model = teacherModel;
    }

    const config: PipelineConfig = {
      run_name: runName.trim(),
      sources: mappedSources,
      chunk: { target_tokens: targetTokens, overlap: chunkOverlap },
      generation,
      validation: { min_length: 20, score_threshold: scoreThreshold },
      limits: { max_records: maxRecords, max_per_source: Math.ceil(maxRecords / validSources.length) },
    };

    mutation.mutate(config);
  };

  const section = "bg-card rounded-xl border border-card-border p-5 space-y-4";
  const sectionTitle = "text-xs font-semibold text-muted-foreground uppercase tracking-wider";

  const genModeDescription: Record<GenMode, string> = {
    qa: "Generate question-answer pairs for fine-tuning",
    instruction: "Generate instruction-response pairs (Alpaca format)",
    summary: "Generate document-summary pairs",
    chat: "Generate multi-turn conversation examples",
    distillation_cot: "Teacher generates step-by-step reasoning traces (instruction + thinking + output)",
    distillation_dpo: "Teacher generates preferred and rejected response pairs (Anthropic HH / TRL DPOTrainer format)",
    distillation_sft: "Teacher generates rich, verbose expert explanations for knowledge transfer",
  };

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <Link href="/">
          <Button variant="ghost" size="sm" className="gap-1.5 text-muted-foreground hover:text-foreground">
            <ArrowLeft className="w-4 h-4" />
            Runs
          </Button>
        </Link>
        <div className="h-4 w-px bg-border" />
        <h1 className="text-xl font-bold text-foreground">New Pipeline Run</h1>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className={section}>
          <h2 className={sectionTitle}>Run Settings</h2>
          <div>
            <Label htmlFor="runName" className="text-sm text-foreground">Run Name</Label>
            <Input
              id="runName"
              value={runName}
              onChange={(e) => setRunName(e.target.value)}
              placeholder="e.g., ml-qa-v1"
              className="mt-1.5 bg-background border-input text-foreground placeholder:text-muted-foreground"
            />
          </div>
        </div>

        <div className={section}>
          <div className="flex items-center justify-between">
            <h2 className={sectionTitle}>Sources</h2>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={addSource}
              className="gap-1 border-border text-muted-foreground hover:text-foreground h-7 text-xs"
            >
              <Plus className="w-3 h-3" />
              Add Source
            </Button>
          </div>

          <div className="space-y-3">
            {sources.map((source, idx) => (
              <div key={source.id} className="border border-border rounded-lg p-4 space-y-3 bg-background/50">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-muted-foreground">Source {idx + 1}</span>
                  {sources.length > 1 && (
                    <button
                      type="button"
                      onClick={() => removeSource(source.id)}
                      className="text-muted-foreground hover:text-red-400 transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>

                <div className={source.type === "crawl" ? "w-full" : "grid grid-cols-2 gap-3"}>
                  <div>
                    <Label className="text-xs text-muted-foreground">Type</Label>
                    <Select
                      value={source.type}
                      onValueChange={(v) => updateSource(source.id, { type: v as SourceType })}
                    >
                      <SelectTrigger className="mt-1.5 h-9 bg-background border-input text-foreground">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="bg-card border-card-border">
                        <SelectItem value="text">Text</SelectItem>
                        <SelectItem value="url">URL</SelectItem>
                        <SelectItem value="crawl">Crawl</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {source.type !== "crawl" && (
                    <div>
                      <Label className="text-xs text-muted-foreground">Title (optional)</Label>
                      <Input
                        value={source.title}
                        onChange={(e) => updateSource(source.id, { title: e.target.value })}
                        placeholder="Descriptive name"
                        className="mt-1.5 h-9 bg-background border-input text-foreground placeholder:text-muted-foreground"
                      />
                    </div>
                  )}
                </div>

                {source.type === "text" && (
                  <div>
                    <Label className="text-xs text-muted-foreground">Text Content</Label>
                    <Textarea
                      value={source.value}
                      onChange={(e) => updateSource(source.id, { value: e.target.value })}
                      placeholder="Paste text to generate training data from..."
                      className="mt-1.5 min-h-28 text-sm bg-background border-input text-foreground placeholder:text-muted-foreground resize-y"
                    />
                  </div>
                )}

                {source.type === "url" && (
                  <div>
                    <Label className="text-xs text-muted-foreground">URL</Label>
                    <Input
                      value={source.value}
                      onChange={(e) => updateSource(source.id, { value: e.target.value })}
                      placeholder="https://example.com/article"
                      className="mt-1.5 bg-background border-input text-foreground placeholder:text-muted-foreground"
                    />
                  </div>
                )}

                {source.type === "crawl" && (
                  <div className="space-y-3">
                    <div>
                      <Label className="text-xs text-muted-foreground">Seed URL</Label>
                      <Input
                        value={source.value}
                        onChange={(e) => updateSource(source.id, { value: e.target.value })}
                        placeholder="https://docs.example.com"
                        className="mt-1.5 bg-background border-input text-foreground placeholder:text-muted-foreground"
                      />
                      <p className="text-xs text-muted-foreground mt-1">
                        The crawler will follow internal links starting from this URL
                      </p>
                    </div>

                    <button
                      type="button"
                      onClick={() => updateSource(source.id, { crawlSettingsOpen: !source.crawlSettingsOpen })}
                      className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
                    >
                      {source.crawlSettingsOpen ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                      Crawler Settings
                    </button>

                    {source.crawlSettingsOpen && (
                      <div className="space-y-3 border border-border rounded-md p-3 bg-background/30">
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <div className="flex items-center justify-between mb-1.5">
                              <Label className="text-xs text-muted-foreground">Max Depth</Label>
                              <span className="text-xs font-semibold text-primary tabular-nums">{source.crawlMaxDepth}</span>
                            </div>
                            <Slider
                              min={1} max={5} step={1}
                              value={[source.crawlMaxDepth]}
                              onValueChange={([v]) => updateSource(source.id, { crawlMaxDepth: v })}
                            />
                            <div className="flex justify-between text-xs text-muted-foreground mt-1">
                              <span>1</span><span>5</span>
                            </div>
                          </div>
                          <div>
                            <div className="flex items-center justify-between mb-1.5">
                              <Label className="text-xs text-muted-foreground">Max Pages</Label>
                              <span className="text-xs font-semibold text-primary tabular-nums">{source.crawlMaxPages}</span>
                            </div>
                            <Slider
                              min={1} max={200} step={1}
                              value={[source.crawlMaxPages]}
                              onValueChange={([v]) => updateSource(source.id, { crawlMaxPages: v })}
                            />
                            <div className="flex justify-between text-xs text-muted-foreground mt-1">
                              <span>1</span><span>200</span>
                            </div>
                          </div>
                        </div>

                        <div>
                          <Label className="text-xs text-muted-foreground">Allowed Domains (optional)</Label>
                          <Input
                            value={source.crawlAllowedDomains}
                            onChange={(e) => updateSource(source.id, { crawlAllowedDomains: e.target.value })}
                            placeholder="docs.example.com, blog.example.com"
                            className="mt-1.5 h-9 bg-background border-input text-foreground placeholder:text-muted-foreground text-xs"
                          />
                          <p className="text-xs text-muted-foreground mt-1">
                            Comma-separated list. Leave blank to restrict to the seed URL's domain.
                          </p>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className={section}>
          <h2 className={sectionTitle}>Generation Settings</h2>

          <div>
            <Label className="text-sm text-foreground">Generation Mode</Label>
            <Select value={genMode} onValueChange={(v) => setGenMode(v as GenMode)}>
              <SelectTrigger className="mt-1.5 bg-background border-input text-foreground">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-card border-card-border">
                <SelectGroup>
                  <SelectLabel className="text-xs text-muted-foreground px-2 py-1">Standard</SelectLabel>
                  <SelectItem value="qa">Q&A Pairs</SelectItem>
                  <SelectItem value="instruction">Instruction Following</SelectItem>
                  <SelectItem value="summary">Summarization</SelectItem>
                  <SelectItem value="chat">Chat / Dialogue</SelectItem>
                </SelectGroup>
                <SelectGroup>
                  <SelectLabel className="text-xs text-muted-foreground px-2 py-1 border-t border-border mt-1 pt-2">Distillation</SelectLabel>
                  <SelectItem value="distillation_cot">Chain-of-Thought (CoT)</SelectItem>
                  <SelectItem value="distillation_dpo">Direct Preference (DPO)</SelectItem>
                  <SelectItem value="distillation_sft">Knowledge Distillation (SFT)</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground mt-1.5">
              {genModeDescription[genMode]}
            </p>
          </div>

          {isDistillationMode(genMode) && (
            <div>
              <Label className="text-sm text-foreground">Teacher Model</Label>
              <Input
                value={teacherModel}
                onChange={(e) => setTeacherModel(e.target.value)}
                placeholder="e.g., gpt-5-mini"
                className="mt-1.5 bg-background border-input text-foreground placeholder:text-muted-foreground"
              />
              <p className="text-xs text-muted-foreground mt-1">
                Model used as the teacher for distillation. Defaults to the configured model.
              </p>
            </div>
          )}

          <div>
            <div className="flex items-center justify-between mb-2">
              <Label className="text-sm text-foreground">Max Total Records</Label>
              <span className="text-sm font-semibold text-primary tabular-nums">{maxRecords}</span>
            </div>
            <Slider
              min={10} max={1000} step={10}
              value={[maxRecords]}
              onValueChange={([v]) => setMaxRecords(v)}
            />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>10</span><span>1000</span>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <Label className="text-sm text-foreground">Records per Chunk</Label>
              <span className="text-sm font-semibold text-primary tabular-nums">{maxPerChunk}</span>
            </div>
            <Slider
              min={1} max={10} step={1}
              value={[maxPerChunk]}
              onValueChange={([v]) => setMaxPerChunk(v)}
            />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>1</span><span>10</span>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <Label className="text-sm text-foreground">Chunk Size (tokens)</Label>
              <span className="text-sm font-semibold text-primary tabular-nums">{targetTokens}</span>
            </div>
            <Slider
              min={50} max={1000} step={50}
              value={[targetTokens]}
              onValueChange={([v]) => setTargetTokens(v)}
            />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>50</span><span>1000</span>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <Label className="text-sm text-foreground">Chunk Overlap (tokens)</Label>
              <span className="text-sm font-semibold text-primary tabular-nums">{chunkOverlap}</span>
            </div>
            <Slider
              min={0} max={Math.min(targetTokens - 10, 400)} step={10}
              value={[Math.min(chunkOverlap, Math.max(0, targetTokens - 10))]}
              onValueChange={([v]) => setChunkOverlap(v)}
            />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>0</span><span>{Math.min(targetTokens - 10, 400)}</span>
            </div>
            <p className="text-xs text-muted-foreground mt-1">Tokens shared between adjacent chunks to preserve context</p>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <Label className="text-sm text-foreground">Min Quality Score</Label>
              <span className="text-sm font-semibold text-primary tabular-nums">{(scoreThreshold * 100).toFixed(0)}%</span>
            </div>
            <Slider
              min={0} max={1} step={0.05}
              value={[scoreThreshold]}
              onValueChange={([v]) => setScoreThreshold(v)}
            />
            <div className="flex justify-between text-xs text-muted-foreground mt-1">
              <span>0%</span><span>100%</span>
            </div>
          </div>
        </div>

        {error && (
          <div className="text-sm text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-3">
            {error}
          </div>
        )}

        <Button
          type="submit"
          className="w-full gap-2 bg-primary text-primary-foreground hover:bg-primary/90 font-semibold py-5"
          disabled={mutation.isPending}
        >
          <Zap className="w-4 h-4" />
          {mutation.isPending ? "Starting Pipeline..." : "Start Pipeline Run"}
        </Button>
      </form>
    </div>
  );
}
