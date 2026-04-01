import { Lightbulb, Download } from 'lucide-react';

export default function InsightsPanel({ insights = [], onExport }) {
  if (!insights || insights.length === 0) {
    return (
      <div className="card text-center py-12">
        <Lightbulb className="w-12 h-12 text-slate-500 mx-auto mb-3" />
        <p className="text-slate-400">💭 No insights available</p>
      </div>
    );
  }

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-yellow-400"></div>
          <h3 className="text-lg font-bold text-white">Key Insights</h3>
        </div>
        {onExport && (
          <button
            onClick={onExport}
            className="btn-secondary text-sm flex items-center gap-2"
          >
            <Download className="w-4 h-4" />
            Export
          </button>
        )}
      </div>

      <div className="space-y-3">
        {Array.isArray(insights) ? (
          insights.map((insight, idx) => (
            <div
              key={idx}
              className="p-4 bg-gradient-to-r from-yellow-500/10 to-amber-500/10 border-l-4 border-yellow-400 rounded-lg hover:bg-gradient-to-r hover:from-yellow-500/15 hover:to-amber-500/15 transition-all"
            >
              <div className="flex gap-3">
                <span className="text-lg flex-shrink-0">✨</span>
                <p className="text-slate-200 leading-relaxed text-sm">{insight}</p>
              </div>
            </div>
          ))
        ) : (
          <div className="p-4 bg-gradient-to-r from-yellow-500/10 to-amber-500/10 border-l-4 border-yellow-400 rounded-lg">
            <div className="flex gap-3">
              <span className="text-lg flex-shrink-0">✨</span>
              <p className="text-slate-200 whitespace-pre-wrap text-sm">{insights}</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
