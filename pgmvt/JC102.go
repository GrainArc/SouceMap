package pgmvt

import (
	"math"
)

type ChangeCoord struct {
	mcband []float64
	mc2ll  [][]float64
	xPi    float64
	pi     float64
	a      float64
	ee     float64
}

// NewChangeCoord 初始化坐标转换对象
func NewChangeCoord() *ChangeCoord {
	return &ChangeCoord{
		mcband: []float64{12890594.86, 8362377.87, 5591021, 3481989.83, 1678043.12, 0},
		mc2ll: [][]float64{
			{1.410526172116255e-8, 0.00000898305509648872, -1.9939833816331, 200.9824383106796, -187.2403703815547,
				91.6087516669843, -23.38765649603339, 2.57121317296198, -0.03801003308653, 17337981.2},
			{-7.435856389565537e-9, 0.000008983055097726239, -0.78625201886289, 96.32687599759846, -1.85204757529826,
				-59.36935905485877, 47.40033549296737, -16.50741931063887, 2.28786674699375, 10260144.86},
			{-3.030883460898826e-8, 0.00000898305509983578, 0.30071316287616, 59.74293618442277, 7.357984074871,
				-25.38371002664745, 13.45380521110908, -3.29883767235584, 0.32710905363475, 6856817.37},
			{-1.981981304930552e-8, 0.000008983055099779535, 0.03278182852591, 40.31678527705744, 0.65659298677277,
				-4.44255534477492, 0.85341911805263, 0.12923347998204, -0.04625736007561, 4482777.06},
			{3.09191371068437e-9, 0.000008983055096812155, 0.00006995724062, 23.10934304144901, -0.00023663490511,
				-0.6321817810242, -0.00663494467273, 0.03430082397953, -0.00466043876332, 2555164.4},
			{2.890871144776878e-9, 0.000008983055095805407, -3.068298e-8, 7.47137025468032, -0.00000353937994,
				-0.02145144861037, -0.00001234426596, 0.00010322952773, -0.00000323890364, 826088.5},
		},
		xPi: 3.14159265358979324 * 3000.0 / 180.0,
		pi:  3.1415926535897932384626,
		a:   6378245.0,
		ee:  0.00669342162296594323,
	}
}

// Convert 平面转经纬计算公式
func (c *ChangeCoord) Convert(lng, lat float64, f []float64) (float64, float64) {
	if len(f) == 0 {
		return 0, 0
	}

	tlng := f[0] + f[1]*math.Abs(lng)
	cc := math.Abs(lat) / f[9]
	tlat := 0.0

	for index := 0; index < 7; index++ {
		tlat += f[index+2] * math.Pow(cc, float64(index))
	}

	if lng < 0 {
		tlng *= -1
	}

	if lat < 0 {
		tlat *= -1
	}

	return tlng, tlat
}

// DB09mcToBD09 百度平面转经纬
func (c *ChangeCoord) DB09mcToBD09(mercartorX, mercartorY float64) (float64, float64) {
	mercartorX = math.Abs(mercartorX)
	mercartorY = math.Abs(mercartorY)

	var f []float64
	index := 0

	for _, mcb := range c.mcband {
		if mercartorY >= mcb {
			f = c.mc2ll[index]
			break
		}
		index++
	}

	if len(f) == 0 {
		index = 0
		for _, mcb := range c.mcband {
			if -mercartorY <= mcb {
				f = c.mc2ll[index]
				break
			}
			index++
		}
	}

	return c.Convert(mercartorX, mercartorY, f)
}

// GCJ02ToBD09 火星坐标系(GCJ-02)转百度坐标系(BD-09)
func (c *ChangeCoord) GCJ02ToBD09(lng, lat float64) (float64, float64) {
	z := math.Sqrt(lng*lng+lat*lat) + 0.00002*math.Sin(lat*c.xPi)
	theta := math.Atan2(lat, lng) + 0.000003*math.Cos(lng*c.xPi)
	bdLng := z*math.Cos(theta) + 0.0065
	bdLat := z*math.Sin(theta) + 0.006
	return bdLng, bdLat
}

// BD09ToGCJ02 百度坐标系(BD-09)转火星坐标系(GCJ-02)
func (c *ChangeCoord) BD09ToGCJ02(lng, lat float64) (float64, float64) {
	x := lng - 0.0065
	y := lat - 0.006
	z := math.Sqrt(x*x+y*y) - 0.00002*math.Sin(y*c.xPi)
	theta := math.Atan2(y, x) - 0.000003*math.Cos(x*c.xPi)
	ggLng := z * math.Cos(theta)
	ggLat := z * math.Sin(theta)
	return ggLng, ggLat
}

// WGS84ToGCJ02 WGS84转GCJ02(火星坐标系)
func (c *ChangeCoord) WGS84ToGCJ02(lng, lat float64) (float64, float64) {
	if c.OutOfChina(lng, lat) {
		return lng, lat
	}

	dlat := c.transformLat(lng-105.0, lat-35.0)
	dlng := c.transformLng(lng-105.0, lat-35.0)
	radlat := lat / 180.0 * c.pi
	magic := math.Sin(radlat)
	magic = 1 - c.ee*magic*magic
	sqrtmagic := math.Sqrt(magic)

	dlat = (dlat * 180.0) / ((c.a * (1 - c.ee)) / (magic * sqrtmagic) * c.pi)
	dlng = (dlng * 180.0) / (c.a / sqrtmagic * math.Cos(radlat) * c.pi)

	mglat := lat + dlat
	mglng := lng + dlng
	return mglng, mglat
}

// GCJ02ToWGS84 GCJ02(火星坐标系)转WGS84---迭代逼近法
func (c *ChangeCoord) GCJ02ToWGS84(lng, lat float64) (float64, float64) {
	if c.OutOfChina(lng, lat) {
		return lng, lat
	}

	initDelta := 0.01
	threshold := 0.000000001
	dlng := initDelta
	dlat := initDelta
	mlng := lng - dlng
	mlat := lat - dlat
	plng := lng + dlng
	plat := lat + dlat

	for i := 0; i < 30; i++ {
		wgslng := (mlng + plng) / 2
		wgslat := (mlat + plat) / 2
		tmplng, tmplat := c.WGS84ToGCJ02(wgslng, wgslat)
		dlng = tmplng - lng
		dlat = tmplat - lat

		if math.Abs(dlng) < threshold && math.Abs(dlat) < threshold {
			return wgslng, wgslat
		}

		if dlng > 0 {
			plng = wgslng
		} else {
			mlng = wgslng
		}

		if dlat > 0 {
			plat = wgslat
		} else {
			mlat = wgslat
		}
	}

	return (mlng + plng) / 2, (mlat + plat) / 2
}

// BD09ToWGS84 bd09转wgs84坐标
func (c *ChangeCoord) BD09ToWGS84(lng, lat float64) (float64, float64) {
	lng, lat = c.BD09ToGCJ02(lng, lat)
	return c.GCJ02ToWGS84(lng, lat)
}

// WGS84ToBD09 wgs84坐标转bd09
func (c *ChangeCoord) WGS84ToBD09(lng, lat float64) (float64, float64) {
	lng, lat = c.WGS84ToGCJ02(lng, lat)
	return c.GCJ02ToBD09(lng, lat)
}

// BD09mcToWGS84 百度平面转wgs84坐标
func (c *ChangeCoord) BD09mcToWGS84(lng, lat float64) (float64, float64) {
	lng, lat = c.DB09mcToBD09(lng, lat)
	lng, lat = c.BD09ToGCJ02(lng, lat)
	return c.GCJ02ToWGS84(lng, lat)
}

// BD09mcToGCJ02 百度平面转gcj02坐标
func (c *ChangeCoord) BD09mcToGCJ02(lng, lat float64) (float64, float64) {
	lng, lat = c.DB09mcToBD09(lng, lat)
	return c.BD09ToGCJ02(lng, lat)
}

// transformLat 计算纬度偏移
func (c *ChangeCoord) transformLat(lng, lat float64) float64 {
	ret := -100.0 + 2.0*lng + 3.0*lat + 0.2*lat*lat +
		0.1*lng*lat + 0.2*math.Sqrt(math.Abs(lng))

	ret += (20.0*math.Sin(6.0*lng*c.pi) + 20.0*
		math.Sin(2.0*lng*c.pi)) * 2.0 / 3.0

	ret += (20.0*math.Sin(lat*c.pi) + 40.0*
		math.Sin(lat/3.0*c.pi)) * 2.0 / 3.0

	ret += (160.0*math.Sin(lat/12.0*c.pi) + 320.0*
		math.Sin(lat*c.pi/30.0)) * 2.0 / 3.0

	return ret
}

// transformLng 计算经度偏移
func (c *ChangeCoord) transformLng(lng, lat float64) float64 {
	ret := 300.0 + lng + 2.0*lat + 0.1*lng*lng +
		0.1*lng*lat + 0.1*math.Sqrt(math.Abs(lng))

	ret += (20.0*math.Sin(6.0*lng*c.pi) + 20.0*
		math.Sin(2.0*lng*c.pi)) * 2.0 / 3.0

	ret += (20.0*math.Sin(lng*c.pi) + 40.0*
		math.Sin(lng/3.0*c.pi)) * 2.0 / 3.0

	ret += (150.0*math.Sin(lng/12.0*c.pi) + 300.0*
		math.Sin(lng/30.0*c.pi)) * 2.0 / 3.0

	return ret
}

// OutOfChina 判断是否在国内
func (c *ChangeCoord) OutOfChina(lng, lat float64) bool {
	return !(lng > 73.66 && lng < 135.05 && lat > 3.86 && lat < 53.55)
}
