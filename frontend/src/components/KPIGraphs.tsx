"use client"

import useSWR from "swr"
import { PieChart, Pie, Cell, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend } from "recharts"

const fetcher = (url: string) => fetch(url).then((res) => res.json())

const COLORS = {
  "En movimiento": "#10b981", // emerald-500
  "Detenido": "#f97316",      // orange-500
  "Alerta": "#ef4444"         // red-500
}

export default function KPIGraphs() {
  const { data, error, isLoading } = useSWR(
    `${process.env.NEXT_PUBLIC_API_URL}/fleet/stats`,
    fetcher,
    { refreshInterval: 10000 }
  )

  if (isLoading) return <div className="h-64 bg-slate-900 animate-pulse rounded-xl border border-slate-800 mb-6"></div>
  if (error || !data) return null

  // Mapeo adaptado para Recharts
  const chartData = data.details.map((d: any) => ({
    name: d.status,
    value: d.count,
    fuel: Math.round(d.avg_fuel_pct)
  }))

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
      
      {/* Gráfico Circular de Distribución */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-sm">
        <h3 className="text-lg font-semibold text-slate-200 mb-4">Distribución de Flota Operativa</h3>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={80}
                paddingAngle={5}
                dataKey="value"
              >
                {chartData.map((entry: any, index: number) => (
                  <Cell key={`cell-${index}`} fill={COLORS[entry.name as keyof typeof COLORS] || "#8884d8"} />
                ))}
              </Pie>
              <RechartsTooltip 
                contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b', color: '#f8fafc', borderRadius: '0.5rem' }}
                itemStyle={{ color: '#f8fafc' }}
              />
              <Legend verticalAlign="bottom" height={36} wrapperStyle={{ paddingBottom: "10px" }}/>
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Gráfico de Barras de Combustible */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-sm">
        <h3 className="text-lg font-semibold text-slate-200 mb-4">Promedio de Combustible por Estado (%)</h3>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis dataKey="name" stroke="#94a3b8" tick={{ fill: '#94a3b8', fontSize: 12 }} />
              <YAxis stroke="#94a3b8" tick={{ fill: '#94a3b8', fontSize: 12 }} domain={[0, 100]} />
              <RechartsTooltip 
                cursor={{ fill: '#1e293b' }}
                contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b', color: '#f8fafc', borderRadius: '0.5rem' }}
              />
              <Bar dataKey="fuel" name="Nivel Promedio (%)" radius={[4, 4, 0, 0]} maxBarSize={60}>
                {chartData.map((entry: any, index: number) => (
                  <Cell key={`cell-${index}`} fill={COLORS[entry.name as keyof typeof COLORS] || "#8884d8"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

    </div>
  )
}
