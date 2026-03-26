"use client"

import { useState } from "react"
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet"
import useSWR from "swr"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

const createIcon = (color: string) => {
  return new L.DivIcon({
    className: 'custom-div-icon',
    html: `<div style="background-color: ${color}; width: 14px; height: 14px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 4px rgba(0,0,0,0.5);"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7]
  })
}

const icons = {
  "En movimiento": createIcon("#10b981"),
  "Detenido": createIcon("#f97316"),
  "Alerta": createIcon("#ef4444")
}

const fetcher = (url: string) => fetch(url).then((res) => res.json())

export default function LiveMap() {
  const [filterId, setFilterId] = useState("")
  const [filterStatus, setFilterStatus] = useState("Todos")

  const { data: locations, error } = useSWR(
    `${process.env.NEXT_PUBLIC_API_URL}/fleet/locations`,
    fetcher,
    { refreshInterval: 10000 }
  )

  const center: [number, number] = [40.7128, -74.0060]

  if (error) return <div className="h-[600px] bg-red-900/20 text-red-500 flex items-center justify-center rounded-xl border border-red-800/50">Error de conexión al cargar el mapa</div>
  if (!locations) return <div className="h-[600px] w-full bg-slate-900 flex items-center justify-center animate-pulse rounded-xl border border-slate-800"><p className="text-slate-500">Renderizando mapa geolocalizado...</p></div>

  // Lógica de filtrado
  const filteredLocations = locations.filter((loc: any) => {
    const matchId = loc.vehicle_id.toLowerCase().includes(filterId.toLowerCase())
    const matchStatus = filterStatus === "Todos" || loc.status === filterStatus
    return matchId && matchStatus
  })

  return (
    <div className="flex flex-col space-y-4">
      
      {/* Barra de Filtros */}
      <div className="flex flex-col sm:flex-row gap-4 bg-slate-900 p-4 rounded-xl border border-slate-800 shadow-sm">
        <div className="flex flex-col space-y-1.5 w-full sm:w-1/3">
          <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Buscar por Unidad ID</label>
          <input 
            type="text" 
            placeholder="Ej. V-001" 
            className="bg-slate-950 border border-slate-700 rounded-md px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
            value={filterId}
            onChange={(e) => setFilterId(e.target.value)}
          />
        </div>
        
        <div className="flex flex-col space-y-1.5 w-full sm:w-1/3">
          <label className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Filtrar por Estado</label>
          <select 
            className="bg-slate-950 border border-slate-700 rounded-md px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value)}
          >
            <option value="Todos">Todos los Estados</option>
            <option value="En movimiento">🟢 En Movimiento</option>
            <option value="Detenido">🟠 Detenido</option>
            <option value="Alerta">🔴 Alerta Crítica</option>
          </select>
        </div>
        
        <div className="flex flex-col justify-end w-full sm:w-1/3">
          <div className="h-9 px-4 flex items-center bg-blue-900/20 text-blue-400 text-sm font-medium border border-blue-800/30 rounded-md">
            Mostrando {filteredLocations.length} de {locations.length} unidades
          </div>
        </div>
      </div>

      {/* Mapa Principal */}
      <div className="h-[600px] w-full rounded-xl overflow-hidden border border-slate-800 shadow-lg relative z-0">
        <MapContainer center={center} zoom={11} className="h-full w-full">
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          />
          {filteredLocations.map((loc: any) => (
            <Marker 
              key={loc.vehicle_id} 
              position={[loc.latitude, loc.longitude]}
              icon={icons[loc.status as keyof typeof icons] || icons["Detenido"]}
            >
              <Popup className="custom-popup" closeButton={false}>
                <div className="font-sans text-slate-800 p-1">
                  <h3 className="font-bold text-sm border-b border-slate-200 pb-1 mb-2">Unidad: {loc.vehicle_id}</h3>
                  <div className="text-xs space-y-1.5">
                    <p><span className="font-semibold text-slate-600">Estado:</span> <span className="font-medium">{loc.status}</span></p>
                    <p><span className="font-semibold text-slate-600">Velocidad:</span> {loc.speed_kmh} km/h</p>
                    <p><span className="font-semibold text-slate-600">Combustible:</span> {loc.fuel_level_pct}%</p>
                    <p><span className="font-semibold text-slate-600">Geohash (Z):</span> {loc.zone_hash}</p>
                    <p><span className="font-semibold text-slate-600">Última lectura:</span> {new Date(loc.last_seen_ts).toLocaleTimeString()}</p>
                  </div>
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
