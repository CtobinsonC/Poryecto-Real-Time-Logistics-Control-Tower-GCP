"use client"

import useSWR from "swr"
import { Truck, AlertTriangle, Clock, Activity } from "lucide-react"

const fetcher = (url: string) => fetch(url).then((res) => res.json())

export default function KPICards() {
  // Polling cada 10 segundos
  const { data, error, isLoading } = useSWR(
    `${process.env.NEXT_PUBLIC_API_URL}/fleet/stats`,
    fetcher,
    { refreshInterval: 10000 }
  )

  if (isLoading) return <div className="animate-pulse h-32 w-full bg-slate-800 rounded-xl" />
  if (error || !data) return <div className="h-32 bg-red-900/20 text-red-500 flex items-center justify-center rounded-xl">Error cargando métricas</div>

  const moving = data.details.find((d: any) => d.status === "En movimiento")?.count || 0
  const stopped = data.details.find((d: any) => d.status === "Detenido")?.count || 0
  const alert = data.details.find((d: any) => d.status === "Alerta")?.count || 0

  const kpis = [
    { title: "Flota Total", value: data.total_vehicles, icon: Truck, color: "text-blue-500" },
    { title: "En Movimiento", value: moving, icon: Activity, color: "text-emerald-500" },
    { title: "Detenidos", value: stopped, icon: Clock, color: "text-orange-500" },
    { title: "Alertas", value: alert, icon: AlertTriangle, color: "text-red-500" }
  ]

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
      {kpis.map((kpi, i) => (
        <div key={i} className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-sm flex items-center space-x-4">
          <div className={`p-3 rounded-full bg-slate-800 ${kpi.color}`}>
            <kpi.icon size={24} />
          </div>
          <div>
            <p className="text-sm font-medium text-slate-400">{kpi.title}</p>
            <h3 className="text-2xl font-bold text-slate-100">{kpi.value}</h3>
          </div>
        </div>
      ))}
    </div>
  )
}
