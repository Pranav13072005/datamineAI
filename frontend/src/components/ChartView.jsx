import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

const COLORS = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#06b6d4'];

export default function ChartView({ data = {}, title = 'Chart' }) {
  if (!data || !data.chart_data) {
    return (
      <div className="card text-center py-12">
        <p className="text-slate-400">📊 No chart data available</p>
      </div>
    );
  }

  const { chart_type, chart_data } = data;
  const chartHeight = 350;

  const renderChart = () => {
    switch (chart_type?.toLowerCase()) {
      case 'bar':
        return (
          <ResponsiveContainer width="100%" height={chartHeight}>
            <BarChart data={chart_data} margin={{ top: 20, right: 30, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.3)" />
              <XAxis dataKey="name" stroke="#94a3b8" angle={-45} textAnchor="end" height={100} />
              <YAxis stroke="#94a3b8" />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(15, 23, 42, 0.95)',
                  border: '1px solid rgba(100, 116, 139, 0.4)',
                  borderRadius: '8px',
                  boxShadow: '0 8px 16px rgba(0, 0, 0, 0.3)',
                }}
                labelStyle={{ color: '#e2e8f0' }}
                cursor={{ fill: 'rgba(59, 130, 246, 0.15)' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              {Object.keys(chart_data[0] || {})
                .filter((key) => key !== 'name')
                .map((key, idx) => (
                  <Bar key={key} dataKey={key} fill={COLORS[idx % COLORS.length]} radius={[8, 8, 0, 0]} />
                ))}
            </BarChart>
          </ResponsiveContainer>
        );

      case 'line':
        return (
          <ResponsiveContainer width="100%" height={chartHeight}>
            <LineChart data={chart_data} margin={{ top: 20, right: 30, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.3)" />
              <XAxis dataKey="name" stroke="#94a3b8" angle={-45} textAnchor="end" height={100} />
              <YAxis stroke="#94a3b8" />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(15, 23, 42, 0.95)',
                  border: '1px solid rgba(100, 116, 139, 0.4)',
                  borderRadius: '8px',
                  boxShadow: '0 8px 16px rgba(0, 0, 0, 0.3)',
                }}
                labelStyle={{ color: '#e2e8f0' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              {Object.keys(chart_data[0] || {})
                .filter((key) => key !== 'name')
                .map((key, idx) => (
                  <Line
                    key={key}
                    type="monotone"
                    dataKey={key}
                    stroke={COLORS[idx % COLORS.length]}
                    dot={{ fill: COLORS[idx % COLORS.length], r: 4 }}
                    activeDot={{ r: 6 }}
                    strokeWidth={2}
                    isAnimationActive={true}
                  />
                ))}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'pie':
        return (
          <ResponsiveContainer width="100%" height={chartHeight}>
            <PieChart>
              <Pie
                data={chart_data}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, value }) => `${name}: ${value}`}
                outerRadius={90}
                fill="#8884d8"
                dataKey="value"
                animationBegin={0}
                animationDuration={800}
              >
                {chart_data.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(15, 23, 42, 0.95)',
                  border: '1px solid rgba(100, 116, 139, 0.4)',
                  borderRadius: '8px',
                  boxShadow: '0 8px 16px rgba(0, 0, 0, 0.3)',
                }}
                labelStyle={{ color: '#e2e8f0' }}
              />
            </PieChart>
          </ResponsiveContainer>
        );

      default:
        return (
          <div className="text-center py-12">
            <p className="text-slate-400">📊 Unsupported chart type: {chart_type}</p>
          </div>
        );
    }
  };

  return (
    <div className="card">
      <div className="flex items-center gap-2 mb-6">
        <div className="w-2 h-2 rounded-full bg-cyan-400"></div>
        <h3 className="text-lg font-bold text-white">{title}</h3>
      </div>
      <div className="bg-slate-800/40 rounded-lg p-4">
        {renderChart()}
      </div>
    </div>
  );
}
