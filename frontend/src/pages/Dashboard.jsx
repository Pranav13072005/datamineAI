import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Share2 } from 'lucide-react';
import {
  QueryBox,
  QueryResult,
  ResultsTable,
  ChartView,
  InsightsPanel,
  DataReportCard,
} from '../components';
import { useDatasets } from '../hooks/useDatasets';
import { useQuery } from '../hooks/useQuery';
import { useExport } from '../hooks/useExport';
import { datasetAPI } from '../services/api';

export default function Dashboard() {
  const { datasetId } = useParams();
  const navigate = useNavigate();
  const { fetchDatasets } = useDatasets();
  const { loading, error, executeQuery } = useQuery();
  const { exportToPDF } = useExport();

  const [currentDataset, setCurrentDataset] = useState(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(null);
  const [history, setHistory] = useState([]);
  const [selectedHistoryId, setSelectedHistoryId] = useState(null);
  const [selectedResult, setSelectedResult] = useState(null);
  const [activeQuestion, setActiveQuestion] = useState(null);

  const [insightsStatus, setInsightsStatus] = useState('processing');
  const [datasetInsights, setDatasetInsights] = useState(null);
  const [insightsGeneratedAt, setInsightsGeneratedAt] = useState(null);
  const [insightsTimeoutMessage, setInsightsTimeoutMessage] = useState(null);

  useEffect(() => {
    (async () => {
      const list = await fetchDatasets();
      const dataset = list.find((d) => d.id === datasetId);
      if (dataset) {
        setCurrentDataset(dataset);
      } else if (list.length === 0) {
        navigate('/');
      }
    })();
  }, [datasetId, fetchDatasets, navigate]);

  useEffect(() => {
    if (!datasetId) return;

    let cancelled = false;

    // Reset UI for the new dataset.
    Promise.resolve().then(() => {
      if (cancelled) return;
      setSelectedResult(null);
      setSelectedHistoryId(null);
      setActiveQuestion(null);
      setHistoryOpen(false);
      setHistoryError(null);
    });

    (async () => {
      setHistoryLoading(true);
      setHistoryError(null);
      try {
        const resp = await datasetAPI.getHistory(datasetId, 20);
        const rows = Array.isArray(resp?.data) ? resp.data : [];
        if (cancelled) return;
        setHistory(rows);
      } catch (err) {
        if (cancelled) return;
        const detail = err?.response?.data?.detail;
        const msg = (typeof detail === 'string' && detail.trim()) ? detail : (err?.message || 'Failed to load history');
        setHistoryError(msg);
        setHistory([]);
      } finally {
        if (!cancelled) setHistoryLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [datasetId]);

  useEffect(() => {
    if (!datasetId) return;

    let cancelled = false;

    // Defer state resets to avoid react-hooks/set-state-in-effect lint rule.
    Promise.resolve().then(() => {
      if (cancelled) return;
      setInsightsStatus('processing');
      setDatasetInsights(null);
      setInsightsGeneratedAt(null);
      setInsightsTimeoutMessage(null);
    });

    const getErrorMessage = (err) => {
      const detail = err?.response?.data?.detail;
      if (typeof detail === 'string' && detail.trim()) return detail;
      const message = err?.message;
      if (typeof message === 'string' && message.trim()) return message;
      return 'Failed to load insights';
    };

    (async () => {
      const startedAt = Date.now();
      while (!cancelled) {
        try {
          const resp = await datasetAPI.getInsights(datasetId);
          const payload = resp?.data || {};
          const status = payload.status || 'processing';

          if (cancelled) return;
          setInsightsStatus(status);
          setDatasetInsights(payload.insights ?? null);
          setInsightsGeneratedAt(payload.generated_at ? new Date(payload.generated_at) : null);

          if (status === 'ready' || status === 'error') return;
        } catch (err) {
          if (cancelled) return;
          setInsightsStatus('error');
          setDatasetInsights({ error: getErrorMessage(err) });
          return;
        }

        if (Date.now() - startedAt >= 30_000) {
          if (cancelled) return;
          setInsightsTimeoutMessage('Timed out while generating insights (30s). Try refreshing in a moment.');
          return;
        }

        await new Promise((r) => setTimeout(r, 2000));
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [datasetId]);

  const handleQuerySubmit = async (question) => {
    if (!currentDataset) return;

    try {
      const queryResultRaw = await executeQuery(currentDataset.id, question);

      // If backend `related_history` is empty (common when DB persistence runs in a
      // background task), derive a best-effort related item from the already-loaded
      // History list so the UX is visibly working.
      const normalize = (s) => {
        const raw = typeof s === 'string' ? s : '';
        return raw
          .trim()
          .toLowerCase()
          .replace(/[_\-]+/g, ' ')
          .replace(/[^a-z0-9\s]/g, '')
          .replace(/\s+/g, ' ');
      };

      const similarity = (a, b) => {
        const aa = normalize(a);
        const bb = normalize(b);
        if (!aa || !bb) return 0;
        if (aa === bb) return 1;
        if (aa.includes(bb) || bb.includes(aa)) return 0.92;
        const aw = aa.split(' ').filter(Boolean);
        const bw = bb.split(' ').filter(Boolean);
        if (!aw.length || !bw.length) return 0;
        const aset = new Set(aw);
        let common = 0;
        for (const w of bw) if (aset.has(w)) common += 1;
        return common / Math.max(aw.length, bw.length);
      };
      const target = normalize(question);

      const backendRelated = Array.isArray(queryResultRaw?.related_history) ? queryResultRaw.related_history : [];
      let queryResult = queryResultRaw;

      if (!backendRelated.length && Array.isArray(history) && history.length) {
        let best = null;
        let bestScore = 0;
        for (const item of history) {
          const qText = item?.question;
          const sc = similarity(qText, target);
          if (sc > bestScore) {
            bestScore = sc;
            best = item;
          }
        }

        const match = bestScore >= 0.78 ? best : null;
        const qh = typeof match?.question === 'string' ? match.question : '';
        const ah =
          (typeof match?.answer_summary === 'string' && match.answer_summary.trim())
            ? match.answer_summary
            : (typeof match?.response_json?.answer === 'string' ? match.response_json.answer.slice(0, 200) : '');

        if (qh && ah) {
          queryResult = {
            ...(queryResultRaw && typeof queryResultRaw === 'object' ? queryResultRaw : {}),
            related_history: [{ question: qh, answer_summary: ah, score: Number(bestScore.toFixed(3)) }],
          };
        }
      }

      setSelectedResult(queryResult);
      setActiveQuestion(question);

      const entryId = `local-${Date.now()}`;
      setSelectedHistoryId(entryId);
      setHistory((prev) => [
        {
          id: entryId,
          question,
          query_type: typeof queryResult?.query_type === 'string' ? queryResult.query_type : 'analytical',
          created_at: new Date().toISOString(),
          answer_summary: typeof queryResult?.answer === 'string' ? queryResult.answer.slice(0, 200) : null,
          response_json: queryResult,
        },
        ...(Array.isArray(prev) ? prev : []),
      ]);
    } catch (err) {
      console.error('Query failed:', err);
    }
  };

  const handleExportPDF = async () => {
    if (!selectedResult) return;

    try {
      const exportData = {
        dataset_name: currentDataset?.name,
        query: activeQuestion || history?.[0]?.question,
        results: selectedResult,
        timestamp: new Date().toISOString(),
      };
      await exportToPDF(exportData);
    } catch (err) {
      alert('Failed to export PDF: ' + err.message);
    }
  };

  if (!currentDataset) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        {/* Background Accents */}
        <div className="fixed inset-0 overflow-hidden pointer-events-none">
          <div className="absolute top-0 right-0 w-96 h-96 bg-blue-500/5 rounded-full blur-3xl"></div>
          <div className="absolute bottom-0 left-0 w-96 h-96 bg-purple-500/5 rounded-full blur-3xl"></div>
        </div>
        <div className="text-center relative">
          <div className="relative w-16 h-16 mx-auto mb-6">
            <div className="absolute inset-0 rounded-full bg-gradient-to-r from-blue-500 to-purple-500 opacity-20 animate-pulse"></div>
            <div className="absolute inset-2 rounded-full border-3 border-transparent border-t-blue-400 border-r-cyan-400 animate-spin"></div>
          </div>
          <p className="text-slate-300 font-medium">Loading dataset...</p>
        </div>
      </div>
    );
  }

  const truncateQuestion = (text, maxLen = 60) => {
    const s = typeof text === 'string' ? text : '';
    if (s.length <= maxLen) return s;
    return s.slice(0, Math.max(0, maxLen - 1)) + '…';
  };

  const formatTimeAgo = (value) => {
    const dt = value instanceof Date ? value : new Date(value);
    if (!(dt instanceof Date) || Number.isNaN(dt.getTime())) return '';

    const diffMs = Date.now() - dt.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    if (diffSec < 60) return 'just now';
    const diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60) return `${diffMin} min ago`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `${diffHr} hr ago`;
    const diffDay = Math.floor(diffHr / 24);
    if (diffDay < 7) return `${diffDay} day${diffDay === 1 ? '' : 's'} ago`;
    const diffWk = Math.floor(diffDay / 7);
    if (diffWk < 5) return `${diffWk} week${diffWk === 1 ? '' : 's'} ago`;
    return dt.toLocaleDateString();
  };

  const queryTypePillClass = (type) => {
    const t = (type || '').toString().toLowerCase();
    if (t === 'analytical') return 'bg-blue-500/10 border border-blue-500/20 text-blue-300';
    if (t === 'descriptive') return 'bg-purple-500/10 border border-purple-500/20 text-purple-300';
    if (t === 'smalltalk') return 'bg-slate-700/40 border border-slate-600/40 text-slate-300';
    if (t === 'anomaly' || t === 'forecast') return 'bg-amber-500/10 border border-amber-500/20 text-amber-300';
    if (t === 'clustering') return 'bg-emerald-500/10 border border-emerald-500/20 text-emerald-300';
    if (t === 'correlation') return 'bg-cyan-500/10 border border-cyan-500/20 text-cyan-300';
    return 'bg-slate-700/40 border border-slate-600/40 text-slate-300';
  };

  const handleSelectRelatedHistory = (relatedQuestion) => {
    const q = typeof relatedQuestion === 'string' ? relatedQuestion.trim() : '';
    if (!q) return;

    setHistoryOpen(true);

    const normalize = (s) => (typeof s === 'string' ? s.trim().toLowerCase() : '');
    const target = normalize(q);
    const match = Array.isArray(history)
      ? history.find((item) => normalize(item?.question) === target)
      : null;

    const payload = match?.response_json;
    if (payload && typeof payload === 'object') {
      setSelectedResult(payload);
      setActiveQuestion(q);
      const id = match?.id;
      setSelectedHistoryId(id ? String(id) : null);
    }
  };

  return (
    <div className="min-h-screen bg-[#020617] text-slate-200">
      {/* Background Accents */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none z-0">
        <div className="absolute top-[-20%] right-[-10%] w-[1000px] h-[1000px] bg-blue-600/5 rounded-full blur-[150px]"></div>
        <div className="absolute bottom-[-20%] left-[-10%] w-[800px] h-[800px] bg-purple-600/5 rounded-full blur-[120px]"></div>
        <div className="absolute top-[40%] left-[50%] transform -translate-x-1/2 w-[600px] h-[400px] bg-cyan-600/5 rounded-full blur-[100px]"></div>
      </div>

      <div className="relative z-10 max-w-[1400px] mx-auto px-6 py-10">
        {/* Header */}
        <div className="mb-10 slide-in">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 p-6 rounded-3xl bg-slate-800/20 border border-slate-700/50 backdrop-blur-xl shadow-2xl">
            <div className="flex items-center gap-6">
              <button
                onClick={() => navigate('/')}
                className="p-3 bg-slate-800/40 border border-slate-700/50 hover:bg-slate-700/60 rounded-xl transition-all hover:scale-105 active:scale-95 text-slate-300 hover:text-white group"
                title="Back to home"
              >
                <ArrowLeft className="w-6 h-6 group-hover:-translate-x-1 transition-transform" />
              </button>
              <div>
                <h1 className="text-3xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 via-cyan-300 to-purple-400 bg-clip-text text-transparent drop-shadow-sm mb-2">
                  {currentDataset.name}
                </h1>
                <div className="flex flex-wrap items-center gap-3">
                  <span className="px-3 py-1 bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs font-semibold rounded-lg flex items-center gap-1.5 shadow-inner">
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse"></span>
                    {currentDataset.rows ? currentDataset.rows.toLocaleString() : '?'} Rows
                  </span>
                  <span className="px-3 py-1 bg-purple-500/10 border border-purple-500/20 text-purple-400 text-xs font-semibold rounded-lg flex items-center gap-1.5 shadow-inner">
                    <span className="w-1.5 h-1.5 rounded-full bg-purple-500"></span>
                    {currentDataset.columns ? currentDataset.columns.toLocaleString() : '?'} Columns
                  </span>
                </div>
              </div>
            </div>
            <button
              onClick={handleExportPDF}
              disabled={!selectedResult}
              className="btn-primary flex items-center gap-2 px-6 py-3 rounded-xl shadow-xl shadow-blue-900/20 disabled:opacity-40 disabled:shadow-none"
            >
              <Share2 className="w-5 h-5" />
              Export Report
            </button>
          </div>
        </div>

        {/* Main Query and Results */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          {/* Main: Centered Query Assistant + Results */}
          <div className={`${historyOpen ? 'lg:col-span-6' : 'lg:col-span-9'} lg:order-2 order-1 space-y-6 slide-in-right`}>
            {/* Centered Query Assistant (GPT-style) */}
            <div className="card-elevated max-w-3xl mx-auto">
              <div className="flex items-start justify-between gap-4 mb-4">
                <div>
                  <h2 className="text-xl font-bold text-white tracking-tight">
                    Ready for your questions
                  </h2>
                  <p className="text-slate-400 text-sm mt-1 max-w-2xl">
                    Ask anything about this dataset — get summaries, charts, and table results.
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setHistoryOpen((v) => !v)}
                    className={`px-2.5 py-1 rounded-full text-xs font-semibold border transition-colors ${
                      historyOpen
                        ? 'bg-purple-500/10 border-purple-500/30 text-purple-200'
                        : 'bg-slate-800/40 border-slate-700/50 text-slate-300 hover:text-white'
                    }`}
                  >
                    History
                  </button>
                  <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-blue-500/10 border border-blue-500/20 text-blue-300">
                    Query
                  </span>
                </div>
              </div>

              {!selectedResult && !loading ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-4">
                  <div className="p-3 rounded-xl bg-slate-800/40 border border-slate-700/50 text-left hover:bg-slate-800/60 transition-colors cursor-default">
                    <p className="text-[11px] font-bold text-blue-400 uppercase tracking-wider mb-1 flex items-center gap-2">
                      <span className="w-1.5 h-1.5 rounded-full bg-blue-400" /> Trends
                    </p>
                    <p className="text-slate-300 font-medium text-sm">"Show me sales over time by region"</p>
                  </div>
                  <div className="p-3 rounded-xl bg-slate-800/40 border border-slate-700/50 text-left hover:bg-slate-800/60 transition-colors cursor-default">
                    <p className="text-[11px] font-bold text-purple-400 uppercase tracking-wider mb-1 flex items-center gap-2">
                      <span className="w-1.5 h-1.5 rounded-full bg-purple-400" /> Insights
                    </p>
                    <p className="text-slate-300 font-medium text-sm">"What are the top 5 performing products?"</p>
                  </div>
                </div>
              ) : null}

              <QueryBox
                onSubmit={handleQuerySubmit}
                loading={loading}
                disabled={!currentDataset}
              />

              <p className="text-[11px] text-slate-500 mt-3">
                Tip: try asking for a chart ("plot", "trend", "compare").
              </p>
            </div>

            {/* Results / Errors */}

            {error && (
              <div className="alert-error fade-in flex items-center gap-3">
                <span className="text-xl">❌</span>
                <div>
                  <p className="font-semibold">Query Error</p>
                  <p className="text-sm opacity-90">{error}</p>
                </div>
              </div>
            )}

            {loading && (
              <div className="card-elevated text-center py-16 fade-in">
                <div className="inline-block">
                  <div className="relative w-16 h-16 mx-auto mb-4">
                    <div className="absolute inset-0 rounded-full bg-gradient-to-r from-blue-500 to-purple-500 opacity-20 animate-pulse"></div>
                    <div className="absolute inset-2 rounded-full border-3 border-transparent border-t-blue-400 border-r-blue-400 animate-spin"></div>
                  </div>
                  <p className="text-slate-300 font-medium">Analyzing your data...</p>
                  <p className="text-slate-500 text-sm mt-2">This may take a few moments</p>
                </div>
              </div>
            )}

            {selectedResult && !loading && (
              <>
                {/* New unified QueryResponse renderer */}
                {'query_type' in selectedResult || 'answer' in selectedResult ? (
                  <div className="fade-in">
                    <QueryResult result={selectedResult} onSelectRelated={handleSelectRelatedHistory} />
                  </div>
                ) : null}

                {/* Insights */}
                {!('query_type' in selectedResult || 'answer' in selectedResult) && selectedResult.insights && (
                  <div className="fade-in">
                    <InsightsPanel
                      insights={selectedResult.insights}
                      onExport={handleExportPDF}
                    />
                  </div>
                )}

                {/* Chart */}
                {!('query_type' in selectedResult || 'answer' in selectedResult) && selectedResult.chart_data && (
                  <div className="fade-in">
                    <ChartView
                      data={{
                        chart_type: selectedResult.chart_type || 'bar',
                        chart_data: selectedResult.chart_data,
                      }}
                      title={
                        selectedResult.chart_title || 'Visualization'
                      }
                    />
                  </div>
                )}

                {/* Table Results */}
                {!('query_type' in selectedResult || 'answer' in selectedResult) && selectedResult.table_data && (
                  <div className="fade-in">
                    <ResultsTable
                      data={selectedResult.table_data}
                      title={selectedResult.table_title || 'Data Results'}
                    />
                  </div>
                )}

                {/* Raw Response (if no structured data) */}
                {!('query_type' in selectedResult || 'answer' in selectedResult) &&
                  !selectedResult.chart_data &&
                  !selectedResult.table_data &&
                  selectedResult.response && (
                    <div className="card fade-in">
                      <h3 className="section-title">Response</h3>
                      <p className="text-slate-300 whitespace-pre-wrap leading-relaxed">
                        {selectedResult.response}
                      </p>
                    </div>
                  )}
              </>
            )}
          </div>

          {/* Right Panel: History (collapsible) */}
          {historyOpen ? (
            <div className="lg:col-span-3 lg:order-3 order-3 space-y-6 slide-in-right">
              <div className="card-elevated h-fit">
                <div className="flex items-center justify-between gap-3 mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-purple-400"></div>
                    <h3 className="text-lg font-bold text-white">History</h3>
                  </div>
                  <span className="text-[11px] text-slate-500">
                    {Array.isArray(history) ? history.length : 0}
                  </span>
                </div>

                {historyError ? (
                  <div className="mb-3 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-200 text-sm">
                    {historyError}
                  </div>
                ) : null}

                {historyLoading ? (
                  <div className="p-4 text-sm text-slate-400">Loading history…</div>
                ) : null}

                {!historyLoading && (!Array.isArray(history) || history.length === 0) ? (
                  <div className="p-4 text-sm text-slate-400">
                    No past queries yet.
                  </div>
                ) : null}

                {!historyLoading && Array.isArray(history) && history.length > 0 ? (
                  <div className="space-y-2 max-h-[520px] overflow-y-auto pr-2 custom-scrollbar">
                    {history.map((item) => {
                      const id = (item && typeof item === 'object' && item.id) ? String(item.id) : '';
                      const q = item?.question;
                      const qt = item?.query_type;
                      const createdAt = item?.created_at;
                      const isSelected = id && selectedHistoryId ? id === selectedHistoryId : false;
                      return (
                        <button
                          key={id || `${q}-${createdAt}`}
                          onClick={() => {
                            const payload = item?.response_json;
                            if (payload && typeof payload === 'object') {
                              setSelectedResult(payload);
                              setActiveQuestion(typeof q === 'string' ? q : null);
                              setSelectedHistoryId(id || null);
                            }
                          }}
                          className={`w-full p-3 text-left text-sm rounded-lg transition-all transform border ${
                            isSelected
                              ? 'bg-blue-500/20 border-blue-500/50'
                              : 'bg-slate-700/30 hover:bg-slate-700/50 border-slate-600/30 hover:border-slate-500/50'
                          }`}
                          title={typeof q === 'string' ? q : ''}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <p className="text-slate-200 font-medium text-sm leading-snug">
                              {truncateQuestion(q)}
                            </p>
                            <span className={`shrink-0 px-2 py-0.5 rounded-full text-[10px] font-semibold ${queryTypePillClass(qt)}`}>
                              {(qt || 'unknown').toString()}
                            </span>
                          </div>
                          <div className="text-[11px] text-slate-500 mt-2">
                            {formatTimeAgo(createdAt)}
                          </div>
                        </button>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}

          {/* Left Rail: Quick Glance + History */}
          <div className="lg:col-span-3 lg:order-1 order-2 space-y-6 slide-in-left">
            {/* Data Report Card (after upload: poll until ready/error) */}
            <div className="max-w-sm lg:max-w-none">
              <DataReportCard
                status={insightsStatus}
                shape={datasetInsights?.shape || {
                  rows: currentDataset?.rows,
                  cols: currentDataset?.columns,
                }}
                missing={datasetInsights?.missing}
                highMissing={datasetInsights?.high_missing}
                correlations={datasetInsights?.correlations}
                distributionFlags={datasetInsights?.distribution_flags}
                duplicates={datasetInsights?.duplicates}
                errorMessage={typeof datasetInsights?.error === 'string' ? datasetInsights.error : null}
                timeoutMessage={insightsTimeoutMessage}
                generatedAt={insightsGeneratedAt}
              />
            </div>

          </div>
        </div>
      </div>
    </div>
  );
}
