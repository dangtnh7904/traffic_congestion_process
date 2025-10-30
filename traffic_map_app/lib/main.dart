import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';

void main() {
  runApp(const TrafficMapApp());
}

class TrafficMapApp extends StatelessWidget {
  const TrafficMapApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Bản đồ giao thông',
      theme: ThemeData(colorScheme: ColorScheme.fromSeed(seedColor: Colors.blue)),
      home: const HanoiMapPage(),
      debugShowCheckedModeBanner: false,
    );
  }
}

class HanoiMapPage extends StatelessWidget {
  const HanoiMapPage({super.key});

  static final _southWest = LatLng(20.95134, 105.759888);
  static final _northEast = LatLng(21.090747, 105.898247);
  static final _mapCenter = LatLng(21.0210435, 105.8290675);

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Bản đồ Hà Nội (OSM)')),
      body: FlutterMap(
        options: MapOptions(
          initialCenter: _mapCenter,
          initialZoom: 12,
          minZoom: 10,
          maxZoom: 18,
          maxBounds: LatLngBounds(_southWest, _northEast),
        ),
        children: [
          TileLayer(
            urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
            userAgentPackageName: 'com.example.traffic_map_app',
          ),
        ],
      ),
    );
  }
}
