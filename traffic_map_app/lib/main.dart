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
  double _currentZoom = 13.5;
  Map<String, Color> _colorConfig = {
    'low': const Color(0xFF00BCD4),
    'medium': const Color(0xFFFF9800),
    'high': const Color(0xFFE91E63),
    'severe': const Color(0xFF9C27B0),
    'default': const Color(0xFF2196F3),
  };

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

      // Lấy bounds và zoom hiện tại của map
      final bounds = _mapController.camera.visibleBounds;
      final zoom = _mapController.camera.zoom;
      
      // Gọi API với bbox và zoom level
      final uri = Uri.parse('$apiBaseUrl/api/traffic/latest').replace(queryParameters: {
        'minLon': bounds.west.toString(),
        'minLat': bounds.south.toString(),
        'maxLon': bounds.east.toString(),
        'maxLat': bounds.north.toString(),
        'zoom': zoom.toString(),
      });

      print('Loading traffic data for zoom $zoom, bbox: ${bounds.west},${bounds.south},${bounds.east},${bounds.north}');
      
      final response = await http.get(uri).timeout(const Duration(seconds: 10));

      if (response.statusCode == 200) {
        final data = json.decode(response.body);
        final remoteColors = _parseColorMetadata(data['metadata']);
        final polylines = _convertGeoJsonToPolylines(data);

        print('Loaded ${polylines.length} polylines');
        
        if (!mounted) return;
        setState(() {
          if (remoteColors != null) {
            _colorConfig = {..._colorConfig, ...remoteColors};
          }
          trafficPolylines = polylines;
          _currentZoom = zoom;
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

      if (geometry == null) {
        continue;
      }

      switch (geometry['type']) {
        case 'LineString':
          final polyline = _buildPolyline(
            geometry['coordinates'] as List,
            properties,
          );
          if (polyline != null) polylines.add(polyline);
          break;
        case 'MultiLineString':
          final lines = geometry['coordinates'] as List;
          for (final line in lines) {
            final polyline = _buildPolyline(line as List, properties);
            if (polyline != null) polylines.add(polyline);
          }
          break;
        default:
          // Ignore other geometry types for now
          break;
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
          return _colorForKey('low');
        case 'medium':
          return _colorForKey('medium');
        case 'high':
          return _colorForKey('high');
        case 'severe':
          return _colorForKey('severe');
        default:
          break;
      }
    }

    // Nếu không có congestion_level, dùng jamfactor
    final jamfactor = properties['jamfactor'] as double?;
    if (jamfactor != null) {
      if (jamfactor < 1.0) {
        return _colorForKey('low');
      } else if (jamfactor < 2.0) {
        return _colorForKey('medium');
      } else if (jamfactor < 3.0) {
        return _colorForKey('high');
      } else {
        return _colorForKey('severe');
      }
    }

    // Dùng speed nếu có
    final speed = properties['speed'] as double?;
    final freeflowspeed = properties['freeflowspeed'] as double?;
    if (speed != null && freeflowspeed != null && freeflowspeed > 0) {
      final speedRatio = speed / freeflowspeed;
      if (speedRatio > 0.7) {
        return _colorForKey('low');
      } else if (speedRatio > 0.4) {
        return _colorForKey('medium');
      } else if (speedRatio > 0.2) {
        return _colorForKey('high');
      } else {
        return _colorForKey('severe');
      }
    }

    // Mặc định
    return _colorForKey('default');
  }

  double _getStrokeWidth(Map<String, dynamic> properties, String? highway) {
    // Độ rộng cơ bản dựa trên loại highway
    double baseWidth = 2.5;
    
    if (highway != null) {
      switch (highway) {
        case 'motorway':
        case 'trunk':
          baseWidth = 4.0;
          break;
        case 'primary':
          baseWidth = 3.5;
          break;
        case 'secondary':
          baseWidth = 3.0;
          break;
        case 'tertiary':
          baseWidth = 2.5;
          break;
        case 'residential':
        case 'living_street':
        case 'unclassified':
          baseWidth = 2.0;
          break;
        default:
          baseWidth = 2.5;
      }
    }

    // Điều chỉnh dựa trên mức độ tắc nghẽn
    final congestionLevel = properties['congestion_level'] as String?;
    if (congestionLevel != null) {
      switch (congestionLevel.toLowerCase()) {
        case 'severe':
        case 'high':
          return baseWidth + 1.0; // Tăng thêm cho đường tắc
        case 'medium':
          return baseWidth + 0.5;
        default:
          return baseWidth;
      }
    }
    return baseWidth;
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
              onPositionChanged: (position, hasGesture) {
                if (hasGesture) {
                  // Reload data when user pans or zooms
                  loadTrafficData();
                }
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
            _LegendItem(color: _colorForKey('low'), label: 'Thông thoáng'),
            _LegendItem(color: _colorForKey('medium'), label: 'Hơi tắc'),
            _LegendItem(color: _colorForKey('high'), label: 'Tắc'),
            _LegendItem(color: _colorForKey('severe'), label: 'Tắc nghẽn'),
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

  Map<String, Color>? _parseColorMetadata(dynamic metadata) {
    if (metadata is Map<String, dynamic>) {
      final colorConfig = metadata['color_config'];
      if (colorConfig is Map) {
        final parsed = <String, Color>{};
        colorConfig.forEach((key, value) {
          final color = _colorFromHex(value?.toString());
          if (key is String && color != null) {
            parsed[key.toLowerCase()] = color;
          }
        });
        return parsed.isEmpty ? null : parsed;
      }
    }
    return null;
  }

  Color? _colorFromHex(String? hex) {
    if (hex == null) return null;
    final normalized = hex.trim().replaceFirst('#', '');
    if (normalized.length != 6 && normalized.length != 8) return null;
    final buffer = StringBuffer();
    if (normalized.length == 6) buffer.write('FF');
    buffer.write(normalized);
    final value = int.tryParse(buffer.toString(), radix: 16);
    if (value == null) return null;
    return Color(value);
  }

  Color _colorForKey(String key) {
    return _colorConfig[key.toLowerCase()] ?? _colorConfig['default']!;
  }

  Polyline? _buildPolyline(List coordinates, Map<String, dynamic> properties) {
    final points = coordinates
        .map((coord) {
          if (coord is List && coord.length >= 2) {
            final lon = (coord[0] as num).toDouble();
            final lat = (coord[1] as num).toDouble();
            return LatLng(lat, lon);
          }
          return null;
        })
        .whereType<LatLng>()
        .toList();

    if (points.length < 2) {
      return null;
    }

    final highway = properties['highway'] as String?;
    final oneway = _parseOneway(properties['oneway']);
    final color = _getColorForTraffic(properties);
    final strokeWidth = _getStrokeWidth(properties, highway);

    return Polyline(
      points: points,
      strokeWidth: strokeWidth,
      color: color,
      borderColor: Colors.white.withOpacity(oneway ? 0.5 : 0.3),
      borderStrokeWidth: 0.5,
    );
  }

  bool _parseOneway(dynamic value) {
    if (value is bool) return value;
    if (value is String) {
      final normalized = value.toLowerCase();
      return normalized == 'true' || normalized == 't' || normalized == '1';
    }
    if (value is num) {
      return value != 0;
    }
    return false;
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
