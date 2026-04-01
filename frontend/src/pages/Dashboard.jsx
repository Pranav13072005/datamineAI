import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Share2 } from 'lucide-react';
import {
  QueryBox,
  ResultsTable,
  ChartView,
  InsightsPanel,
} from '../components';
import { useDatasets } from '../hooks/useDatasets';
import { useQuery } from '../hooks/useQuery';
import { useExport } from '../hooks/useExport';

export default function Dashboard() {
  const { datasetId } = useParams();
  const navigate = useNavigate();
  const { datasets, fetchDatasets } = useDatasets();
  const { result, loading, error, executeQuery, clearResult } = useQuery();
  const { exportToPDF } = useExport();

  const [currentDataset, setCurrentDataset] = useState(null);
  const [history, setHistory] = useState([]);
  const [selectedResult, setSelectedResult] = useState(null);

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

  const handleQuerySubmit = async (question) => {
    if (!currentDataset) return;

    try {
      const queryResult = await executeQuery(currentDataset.id, question);
      setSelectedResult(queryResult);
      setHistory([
        { question, result: queryResult, timestamp: new Date() },
        ...history,
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
        query: history[0]?.question,
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
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Left: Query Input and History */}
          <div className="lg:col-span-1 space-y-6 slide-in-left">
            {/* Query Section */}
            <div className="card-elevated">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-2 h-2 rounded-full bg-blue-400"></div>
                <h2 className="text-lg font-bold text-white">Query Assistant</h2>
              </div>
              <QueryBox
                onSubmit={handleQuerySubmit}
                loading={loading}
                disabled={!currentDataset}
              />
            </div>

            {/* Query History */}
            {history.length > 0 && (
              <div className="card-elevated h-fit">
                <div className="flex items-center gap-2 mb-4">
                  <div className="w-2 h-2 rounded-full bg-purple-400"></div>
                  <h3 className="text-lg font-bold text-white">Recent Queries</h3>
                </div>
                <div className="space-y-2 max-h-80 overflow-y-auto pr-2 custom-scrollbar">
                  {history.slice(0, 10).map((item, idx) => (
                    <button
                      key={idx}
                      onClick={() => setSelectedResult(item.result)}
                      className={`w-full p-3 text-left text-sm rounded-lg transition-all transform ${
                        selectedResult === item.result
                          ? 'bg-blue-500/30 border border-blue-500/60 scale-105'
                          : 'bg-slate-700/40 hover:bg-slate-700/60 border border-slate-600/40 hover:border-slate-500/60'
                      }`}
                    >
                      <p className="text-slate-200 line-clamp-2 font-medium">{item.question}</p>
                      <p className="text-xs text-slate-400 mt-2">
                        {item.timestamp.toLocaleTimeString()}
                      </p>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Right: Results Display */}
          <div className="lg:col-span-3 space-y-6 slide-in-right">
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
                {/* Insights */}
                {selectedResult.insights && (
                  <div className="fade-in">
                    <InsightsPanel
                      insights={selectedResult.insights}
                      onExport={handleExportPDF}
                    />
                  </div>
                )}

                {/* Chart */}
                {selectedResult.chart_data && (
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
                {selectedResult.table_data && (
                  <div className="fade-in">
                    <ResultsTable
                      data={selectedResult.table_data}
                      title={selectedResult.table_title || 'Data Results'}
                    />
                  </div>
                )}

                {/* Raw Response (if no structured data) */}
                {!selectedResult.chart_data &&
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

            {!selectedResult && !loading && (
              <div className="card-elevated text-center py-24 fade-in flex flex-col items-center justify-center border-dashed border-2 border-slate-700">
                <div className="w-20 h-20 mb-6 rounded-2xl bg-gradient-to-tr from-blue-500/20 to-cyan-500/20 border border-blue-500/20 flex items-center justify-center shadow-inner">
                  <span className="text-4xl filter drop-shadow-md">✨</span>
                </div>
                <h2 className="text-2xl font-bold text-white mb-2 tracking-tight">
                  Ready for your questions
                </h2>
                <p className="text-slate-400 text-base max-w-md mx-auto mb-8">
                  Type a question in the assistant panel to instantly generate AI insights, dynamic charts, and data tables.
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full max-w-xl">
                  <div className="p-4 rounded-xl bg-slate-800/40 border border-slate-700/50 text-left hover:bg-slate-800/60 transition-colors cursor-default">
                    <p className="text-xs font-bold text-blue-400 uppercase tracking-wider mb-2 flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-blue-400"></div> Trends</p>
                    <p className="text-slate-300 font-medium">"Show me sales over time by region"</p>
                  </div>
                  <div className="p-4 rounded-xl bg-slate-800/40 border border-slate-700/50 text-left hover:bg-slate-800/60 transition-colors cursor-default">
                    <p className="text-xs font-bold text-purple-400 uppercase tracking-wider mb-2 flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-purple-400"></div> Insights</p>
                    <p className="text-slate-300 font-medium">"What are the top 5 performing products?"</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
