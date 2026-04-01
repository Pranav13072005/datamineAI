import Plot from 'react-plotly.js';

function normalizeInsights(insights) {
  if (!insights) return [];
  if (Array.isArray(insights)) return insights.filter(Boolean).map(String);
  return [String(insights)];
}

function getPlotProps(chart) {
  if (!chart || !chart.data || typeof chart.data !== 'object') return null;

  const spec = chart.data;
  if (Array.isArray(spec.data) || spec.layout || spec.config) {
    return {
      data: Array.isArray(spec.data) ? spec.data : [],
      layout: spec.layout || {},
      config: spec.config || { responsive: true, displayModeBar: false },
    };
  }

  // Some backends may send a single trace or a partial object.
  return {
    data: Array.isArray(spec) ? spec : [spec],
    layout: { autosize: true },
    config: { responsive: true, displayModeBar: false },
  };
}

export default function QueryResult({ result }) {
  if (!result || typeof result !== 'object') return null;

  const answer = typeof result.answer === 'string' ? result.answer : (typeof result.response === 'string' ? result.response : '');
  const table = result.table && typeof result.table === 'object' ? result.table : null;
  const insights = normalizeInsights(result.insights);
  const warnings = Array.isArray(result.warnings) ? result.warnings.filter(Boolean).map(String) : [];
  const chart = result.chart && typeof result.chart === 'object' ? result.chart : null;

  const showTable = !!(table && Array.isArray(table.columns) && Array.isArray(table.rows));
  const showChart = !!(chart && typeof chart.type === 'string' && chart.type);
  const plotProps = showChart ? getPlotProps(chart) : null;
  const maxRows = 100;
  const tableRows = showTable ? table.rows.slice(0, maxRows) : [];
  const truncated = showTable ? table.rows.length > maxRows : false;

  // Render nothing if this doesn't look like a QueryResponse.
  const looksLikeQueryResponse =
    'answer' in result || 'table' in result || 'chart' in result || 'insights' in result || 'warnings' in result || 'query_type' in result;
  if (!looksLikeQueryResponse) return null;

  return (
    <div className="space-y-6">
      {/* Answer + Insights (single block for consistency) */}
      {answer || insights.length > 0 ? (
        <div className="card fade-in">
          <h3 className="section-title">Answer</h3>
          {answer ? (
            <p className="text-slate-300 whitespace-pre-wrap leading-relaxed">
              {answer}
            </p>
          ) : null}

          {insights.length > 0 ? (
            <div className={answer ? 'mt-4' : ''}>
              <div className="text-sm font-semibold text-slate-200 mb-2">Insights</div>
              <ul className="list-disc pl-5 space-y-2 text-slate-300">
                {insights.map((insight, idx) => (
                  <li key={idx} className="leading-relaxed">
                    {insight}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      ) : null}

      {/* Table */}
      {showTable ? (
        <div className="card fade-in">
          <div className="flex items-center justify-between mb-4">
            <h3 className="section-title">Table</h3>
            {truncated ? (
              <span className="text-xs px-2 py-1 bg-slate-700/50 text-slate-300 rounded">
                showing first {maxRows} rows
              </span>
            ) : null}
          </div>

          <div className="overflow-x-auto rounded-lg border border-slate-700/50">
            <table className="w-full text-sm">
              <thead className="bg-gradient-to-r from-slate-700/50 to-slate-600/30 border-b border-slate-700">
                <tr>
                  {table.columns.map((col, idx) => (
                    <th
                      key={idx}
                      className="px-4 py-4 text-left font-semibold text-slate-200 whitespace-nowrap bg-slate-700/30"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/50">
                {tableRows.map((row, rowIdx) => (
                  <tr key={rowIdx} className="hover:bg-slate-700/20 transition-colors">
                    {table.columns.map((_, colIdx) => {
                      const value = Array.isArray(row) ? row[colIdx] : undefined;
                      return (
                        <td key={colIdx} className="px-4 py-3 text-slate-300">
                          {value !== null && value !== undefined ? (
                            String(value)
                          ) : (
                            <span className="text-slate-500 italic">—</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}

      {/* Chart */}
      {showChart && plotProps ? (
        <div className="card fade-in">
          <h3 className="section-title">Chart</h3>
          <div className="mt-4">
            <Plot
              data={plotProps.data}
              layout={{
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: { color: '#cbd5e1' },
                margin: { l: 40, r: 20, t: 20, b: 40 },
                ...plotProps.layout,
              }}
              config={plotProps.config}
              style={{ width: '100%', height: '380px' }}
              useResizeHandler
            />
          </div>
        </div>
      ) : null}

      {/* Warnings */}
      {warnings.length > 0 ? (
        <div className="fade-in flex flex-wrap gap-2">
          {warnings.map((w, idx) => (
            <span
              key={idx}
              className="px-2.5 py-1 rounded-full text-xs font-semibold bg-amber-500/10 border border-amber-500/20 text-amber-300"
              title={w}
            >
              {w}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}
