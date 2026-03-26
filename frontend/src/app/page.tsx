"use client"

import dynamic from "next/dynamic"
import KPICards from "@/components/KPICards"

// Leaflet requiere acceso directo al objeto 'window' del navegador.
// En Next.js App Router (Server components by default), forzamos la carga exclusiva en Cliente:
const LiveMap = dynamic(() => import("@/components/LiveMap"), { 
  ssr: false,
  loading: () => (
    <div className="h-[600px] w-full bg-slate-900 flex items-center justify-center animate-pulse rounded-xl border border-slate-800">
      <span className="text-slate-500 font-medium">Iniciando motor de mapas geoespacial...</span>
    </div>
  )
})

export default function Home() {
  return (
    <main className="min-h-screen bg-slate-950 text-slate-50 p-6 font-sans">
      <div className="max-w-7xl mx-auto space-y-6">
        
        {/* Cabecera del Dashboard */}
        <header className="pb-4 border-b border-slate-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-white mb-1">
              Torre de Control <span className="text-blue-500">Logística</span>
            </h1>
            <p className="text-slate-400 text-sm">
              Monitoreo en tiempo real de la flota motorizada (Powered by GCP & dbt)
            </p>
          </div>
          <div className="flex items-center space-x-2 bg-slate-900 px-3 py-1.5 rounded-full border border-slate-800 shadow-sm">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
            <span className="text-xs font-semibold text-emerald-500 tracking-wider">LIVE 10s SYNC</span>
          </div>
        </header>

        {/* Contenido Principal */}
        <section>
          {/* Tarjetas Superiores */}
          <KPICards />
          
          {/* Mapa Analítico */}
          <LiveMap />
        </section>
        
      </div>
    </main>
  )
}
