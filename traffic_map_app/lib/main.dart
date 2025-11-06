import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;

void main() {
  runApp(const TrafficMapApp());
}

class TrafficMapApp extends StatelessWidget {
  const TrafficMapApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Bản đồ giao thông Hà Nội',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.blue),
        useMaterial3: true,
      ),
      home: const HanoiMapPage(),
      debugShowCheckedModeBanner: false,
    );
  }
}

class HanoiMapPage extends StatefulWidget {
  const HanoiMapPage({super.key});

  @override
  State<HanoiMapPage> createState() => _HanoiMapPageState();
}

class _HanoiMapPageState extends State<HanoiMapPage> {
  static final _mapCenter = LatLng(21.0131265, 105.8455465);

  List<Polyline> trafficPolylines = [];
  bool isLoading = true;
  String errorMessage = '';
  Map<String, dynamic>? stats;
  Timer? _refreshTimer;
  final MapController _mapController = MapController();

  // API endpoint - 10.0.2.2 cho Android Emulator trỏ tới localhost của máy host
  static const String apiBaseUrl = 'http://10.0.2.2:3000';

  @override
  void initState() {
    super.initState();
    // Không load ngay, đợi map ready
    // loadTrafficData();
    
    // Tự động refresh mỗi 30 giây
    _refreshTimer = Timer.periodic(const Duration(seconds: 30), (_) {
      loadTrafficData();
    });
  }

  @override
  void dispose() {
    _refreshTimer?.cancel();
    super.dispose();
  }

  Future<void> loadTrafficData() async {
    try {
      if (!mounted) return;
      
      setState(() {
        isLoading = true;
        errorMessage = '';
      });

      // Gọi API không có bbox - lấy tất cả data
      final uri = Uri.parse('$apiBaseUrl/api/traffic/latest');

      print('Loading all traffic data...');
      
      final response = await http.get(uri).timeout(const Duration(seconds: 10));

      if (response.statusCode == 200) {
        final data = json.decode(response.body);
        final polylines = _convertGeoJsonToPolylines(data);

        print('Loaded ${polylines.length} polylines');
        
        if (!mounted) return;
        setState(() {
          trafficPolylines = polylines;
          isLoading = false;
        });

        // Load stats
        loadStats();
      } else {
        if (!mounted) return;
        setState(() {
          errorMessage = 'Lỗi: ${response.statusCode}';
          isLoading = false;
        });
      }
    } catch (e) {
      print('Error loading traffic data: $e');
      if (!mounted) return;
      setState(() {
        errorMessage = 'Không thể kết nối tới server: $e';
        isLoading = false;
      });
    }
  }

  Future<void> loadStats() async {
    try {
      final response = await http.get(
        Uri.parse('$apiBaseUrl/api/stats'),
      ).timeout(const Duration(seconds: 5));

      if (response.statusCode == 200) {
        final data = json.decode(response.body);
        setState(() {
          stats = data;
        });
      }
    } catch (e) {
      print('Lỗi khi load stats: $e');
    }
  }

  List<Polyline> _convertGeoJsonToPolylines(Map<String, dynamic> geojson) {
    final features = geojson['features'] as List;
    final polylines = <Polyline>[];

    for (var feature in features) {
      final geometry = feature['geometry'];
      final properties = feature['properties'];

      if (geometry['type'] == 'LineString') {
        final coordinates = geometry['coordinates'] as List;
        final points = coordinates
            .map((coord) => LatLng(coord[1] as double, coord[0] as double))
            .toList();

        // Xác định màu sắc dựa trên congestion_level hoặc jamfactor
        final color = _getColorForTraffic(properties);
        final strokeWidth = _getStrokeWidth(properties);

        polylines.add(
          Polyline(
            points: points,
            strokeWidth: strokeWidth,
            color: color,
            borderColor: Colors.white.withOpacity(0.5), // Viền trắng để nổi bật
            borderStrokeWidth: 1.5,
          ),
        );
      }
    }

    return polylines;
  }

  Color _getColorForTraffic(Map<String, dynamic> properties) {
    // Ưu tiên dùng congestion_level nếu có
    final congestionLevel = properties['congestion_level'] as String?;
    if (congestionLevel != null) {
      switch (congestionLevel.toLowerCase()) {
        case 'low':
          return const Color(0xFF00BCD4); // Xanh dương Cyan - thông thoáng
        case 'medium':
          return const Color(0xFFFF9800); // Cam đậm - hơi tắc
        case 'high':
          return const Color(0xFFE91E63); // Hồng đậm - tắc
        case 'severe':
          return const Color(0xFF9C27B0); // Tím - tắc nghẽn nghiêm trọng
        default:
          break;
      }
    }

    // Nếu không có congestion_level, dùng jamfactor
    final jamfactor = properties['jamfactor'] as double?;
    if (jamfactor != null) {
      if (jamfactor < 2.0) {
        return const Color(0xFF00BCD4); // Xanh dương Cyan - thông thoáng
      } else if (jamfactor < 4.0) {
        return const Color(0xFFFF9800); // Cam đậm - hơi tắc
      } else if (jamfactor < 7.0) {
        return const Color(0xFFE91E63); // Hồng đậm - tắc
      } else {
        return const Color(0xFF9C27B0); // Tím - tắc nghẽn
      }
    }

    // Dùng speed nếu có
    final speed = properties['speed'] as double?;
    final freeflowspeed = properties['freeflowspeed'] as double?;
    if (speed != null && freeflowspeed != null && freeflowspeed > 0) {
      final speedRatio = speed / freeflowspeed;
      if (speedRatio > 0.7) {
        return const Color(0xFF00BCD4); // Xanh dương Cyan
      } else if (speedRatio > 0.4) {
        return const Color(0xFFFF9800); // Cam đậm
      } else if (speedRatio > 0.2) {
        return const Color(0xFFE91E63); // Hồng đậm
      } else {
        return const Color(0xFF9C27B0); // Tím
      }
    }

    // Mặc định
    return const Color(0xFF2196F3); // Xanh dương
  }

  double _getStrokeWidth(Map<String, dynamic> properties) {
    final congestionLevel = properties['congestion_level'] as String?;
    if (congestionLevel != null) {
      switch (congestionLevel.toLowerCase()) {
        case 'severe':
        case 'high':
          return 6.0; // Tăng mạnh từ 6.0
        case 'medium':
          return 5.0; // Tăng từ 5.0
        default:
          return 4.0; // Tăng từ 4.0
      }
    }
    return 4.5; // Tăng từ 4.5
  }

  @override
  Widget build(BuildContext context) {
    // Hiển thị polylines luôn (không check zoom nữa để đơn giản)
    return Scaffold(
      appBar: AppBar(
        title: const Text('Bản đồ Giao thông Hà Nội'),
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: loadTrafficData,
            tooltip: 'Làm mới',
          ),
          IconButton(
            icon: const Icon(Icons.info_outline),
            onPressed: () => _showLegend(context),
            tooltip: 'Chú thích',
          ),
        ],
      ),
      body: Stack(
        children: [
          FlutterMap(
            mapController: _mapController,
            options: MapOptions(
              initialCenter: _mapCenter,
              initialZoom: 13.5,
              minZoom: 11,
              maxZoom: 18,
              onMapReady: () {
                print('Map ready, loading traffic data');
                loadTrafficData();
              },
            ),
            children: [
              TileLayer(
                urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                userAgentPackageName: 'com.example.traffic_map_app',
              ),
              PolylineLayer(
                polylines: trafficPolylines,
              ),
            ],
          ),
          // Hiển thị thông báo zoom khi chưa đủ mức (tạm thời disable)
          // if (!shouldShowTraffic && !isLoading)
          //   Positioned(...),
          if (isLoading)
            const Center(
              child: Card(
                child: Padding(
                  padding: EdgeInsets.all(20.0),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      CircularProgressIndicator(),
                      SizedBox(height: 16),
                      Text('Đang tải dữ liệu giao thông...'),
                    ],
                  ),
                ),
              ),
            ),
          if (errorMessage.isNotEmpty)
            Positioned(
              top: 16,
              left: 16,
              right: 16,
              child: Card(
                color: Colors.red.shade100,
                child: Padding(
                  padding: const EdgeInsets.all(12.0),
                  child: Row(
                    children: [
                      const Icon(Icons.error, color: Colors.red),
                      const SizedBox(width: 8),
                      Expanded(child: Text(errorMessage)),
                      IconButton(
                        icon: const Icon(Icons.close),
                        onPressed: () => setState(() => errorMessage = ''),
                      ),
                    ],
                  ),
                ),
              ),
            ),
          if (stats != null && !isLoading)
            Positioned(
              bottom: 16,
              left: 16,
              child: Card(
                child: Padding(
                  padding: const EdgeInsets.all(12.0),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Text(
                        'Thống kê',
                        style: Theme.of(context).textTheme.titleSmall,
                      ),
                      const SizedBox(height: 8),
                      Text('Số đường: ${stats!['total_streets'] ?? 0}'),
                      Text('Tốc độ TB: ${(stats!['avg_speed'] ?? 0).toStringAsFixed(1)} km/h'),
                      if (stats!['latest_update'] != null)
                        Text('Cập nhật: ${_formatTime(stats!['latest_update'])}'),
                    ],
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }

  void _showLegend(BuildContext context) {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Chú thích màu sắc'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            _LegendItem(color: const Color(0xFF00BCD4), label: 'Thông thoáng'),
            _LegendItem(color: const Color(0xFFFF9800), label: 'Hơi tắc'),
            _LegendItem(color: const Color(0xFFE91E63), label: 'Tắc'),
            _LegendItem(color: const Color(0xFF9C27B0), label: 'Tắc nghẽn'),
            const SizedBox(height: 16),
            const Text(
              'Dữ liệu tự động cập nhật mỗi 30 giây',
              style: TextStyle(fontSize: 12, fontStyle: FontStyle.italic),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Đóng'),
          ),
        ],
      ),
    );
  }

  String _formatTime(String isoTime) {
    try {
      final dt = DateTime.parse(isoTime);
      return '${dt.hour}:${dt.minute.toString().padLeft(2, '0')}';
    } catch (e) {
      return isoTime;
    }
  }
}

class _LegendItem extends StatelessWidget {
  final Color color;
  final String label;

  const _LegendItem({required this.color, required this.label});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4.0),
      child: Row(
        children: [
          Container(
            width: 40,
            height: 4,
            decoration: BoxDecoration(
              color: color,
              borderRadius: BorderRadius.circular(2),
            ),
          ),
          const SizedBox(width: 12),
          Text(label),
        ],
      ),
    );
  }
}
