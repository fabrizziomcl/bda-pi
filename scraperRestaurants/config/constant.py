"""Geographic and category constants used by the orchestrator."""

from __future__ import annotations

from typing import Final

# Target business categories.
categories: Final[list[str]] = [
    "Pollería", "Cevichería", "Pizzería", "Chifa", "Makis",
    "Comida oriental", "Comida peruana", "Comida rapida", "Fastfood",
    "Parrillas", "Pastelería", "Panadería", "Heladería",
    "Comida vegana", "Comida vegetariana", "Comida marina",
    "Comida fusión", "Comida mediterránea", "Broaster", "Salchipapa",
    "Sandwichería", "Hamburguesas", "Hamburguesería",
]

# Peruvian departments (regions).
regions: Final[list[str]] = [
    "Amazonas", "Áncash", "Apurímac", "Arequipa", "Ayacucho", "Cajamarca",
    "Callao", "Cusco", "Huancavelica", "Huánuco", "Ica", "Junín",
    "La Libertad", "Lambayeque", "Lima", "Loreto", "Madre de Dios",
    "Moquegua", "Pasco", "Piura", "Puno", "San Martín", "Tacna",
    "Tumbes", "Ucayali",
]

# Canonical department → provinces mapping.
provinces: Final[dict[str, list[str]]] = {
    "Amazonas": ["Chachapoyas", "Bagua", "Bongará", "Condorcanqui", "Luya", "Rodríguez de Mendoza", "Utcubamba"],
    "Ancash": ["Huaraz", "Chimbote", "Aija", "Antonio Raymondi", "Asunción", "Bolognesi", "Carhuaz", "Carlos F. Fitzcarrald", "Casma", "Corongo", "Huari", "Huarmey", "Huaylas", "Mariscal Luzuriaga", "Ocros", "Pallasca", "Pomabamba", "Recuay", "Santa", "Sihuas", "Yungay"],
    "Apurímac": ["Abancay", "Antabamba", "Aymaraes", "Cotabambas", "Grau", "Chincheros", "Andahuaylas"],
    "Arequipa": ["Arequipa", "Camaná", "Caravelí", "Castilla", "Caylloma", "Condesuyos", "Islay", "La Unión"],
    "Ayacucho": ["Cangallo", "Huanta", "Huamanga", "Huanca Sancos", "La Mar", "Lucanas", "Parinacochas", "Páucar del Sara Sara", "Sucre", "Víctor Fajardo", "Vilcashuamán"],
    "Cajamarca": ["Cajamarca", "Cajabamba", "Celendín", "Chota", "Contumazá", "Cutervo", "Hualgayoc", "Jaén", "San Ignacio", "San Marcos", "San Miguel", "San Pablo", "Santa Cruz"],
    "Callao": ["Callao"],
    "Cusco": ["Cuzco", "Acomayo", "Anta", "Calca", "Canas", "Canchis", "Chumbivilcas", "Espinar", "La Convención", "Paruro", "Paucartambo", "Quispicanchi", "Urubamba"],
    "Huancavelica": ["Huancavelica", "Acobamba", "Angaraes", "Castrovirreyna", "Churcampa", "Huaytará", "Tayacaja"],
    "Huánuco": ["Huánuco", "Ambo", "Dos de Mayo", "Huacaybamba", "Huamalíes", "Leoncio Prado", "Marañón", "Pachitea", "Puerto Inca", "Lauricocha", "Yarowilca"],
    "Ica": ["Ica", "Chincha", "Nazca", "Palpa", "Pisco"],
    "Junín": ["Chanchamayo", "Chupaca", "Concepción", "Huancayo", "Jauja", "Junín", "Satipo", "Tarma", "Yauli"],
    "La Libertad": ["Trujillo", "Ascope", "Bolívar", "Chepén", "Julcán", "Otuzco", "Gran Chimú", "Pacasmayo", "Pataz", "Sánchez Carrión", "Santiago de Chuco", "Virú"],
    "Lambayeque": ["Chiclayo", "Ferreñafe", "Lambayeque"],
    "Lima": ["Barranca", "Cajatambo", "Canta", "Cañete", "Huaral", "Huarochirí", "Huaura", "Lima", "Oyón", "Yauyos"],
    "Loreto": ["Maynas", "Putumayo", "Alto Amazonas", "Loreto", "Mariscal Ramón Castilla", "Requena", "Ucayali", "Datem del Marañón"],
    "Madre de Dios": ["Tambopata", "Manu", "Tahuamanu"],
    "Moquegua": ["Mariscal Nieto", "General Sánchez Cerro", "Ilo"],
    "Pasco": ["Pasco", "Oxapampa", "Daniel A. Carrión"],
    "Piura": ["Ayabaca", "Huancabamba", "Morropón", "Piura", "Sechura", "Sullana", "Paita", "Talara"],
    "Puno": ["San Román", "Puno", "Azángaro", "Chucuito", "El Collao", "Melgar", "Carabaya", "Huancané", "Sandia", "San Antonio de Putina", "Lampa", "Yunguyo", "Moho"],
    "San Martín": ["Bellavista", "El Dorado", "Huallaga", "Lamas", "Mariscal Cáceres", "Moyobamba", "Picota", "Rioja", "San Martín", "Tocache"],
    "Tacna": ["Tacna", "Candarave", "Jorge Basadre", "Tarata"],
    "Tumbes": ["Tumbes", "Zarumilla", "Contralmirante Villar"],
    "Ucayali": ["Coronel Portillo", "Atalaya", "Padre Abad", "Purús"],
}
