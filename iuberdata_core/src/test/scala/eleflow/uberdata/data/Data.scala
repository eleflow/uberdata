package eleflow.uberdata.data

/**
 * Created by dirceu on 03/11/14.
 */
case class Data(id: Int, int: Int, string2: String, double: Double) extends Serializable

case class TrainData(id: Int, int: Int, string2: String, double: Double, string: String, string3: String) extends Serializable

case class TestData(id: Int, string2: String, double: Double, string: String, string3: String) extends Serializable

case class Avazu(id: BigDecimal, click: Long, hour: Int, C1: Int, banner_pos: Int, site_id: String, site_domain: String, site_category: String,
                 app_id: String, app_domain: String, app_category: String, device_id: String, device_ip: String, device_model: String, device_type: Int,
                 device_conn_type: Int, C14: Int, C15: Int, C16: Int, C17: Int, C18: Int, C19: Int)

case class AvazuTest(id: BigDecimal, hour: Int, C1: Int, banner_pos: Int, site_id: String, site_domain: String, site_category: String,
                     app_id: String, app_domain: String, app_category: String, device_id: String, device_ip: String, device_model: String, device_type: Int,
                     device_conn_type: Int, C14: Int, C15: Int, C16: Int, C17: Int, C18: Int, C19: Int)

case class AvazuResumed(id: BigDecimal, click: Long, hour: Int, day: Int, device_model: String, device_type: Int, C19: Int)

case class AvazuTestResumed(id: BigDecimal, hour: Int, day: Int,device_model: String, device_type: Int, C19: Int)

//1.00000941815109427E18	0	14102100	1005	0	1fbe01fe	f3845767	28905ebd	ecad2386	7801e8d9	07d7df22	a99f214a	ddd2926e	44956a24	1	2	15706	320	50	1722	0	35	-1	79
//1.0000169349117864E19	0	14102100	1005	0	1fbe01fe	f3845767	28905ebd	ecad2386	7801e8d9	07d7df22	a99f214a	96809ac8	711ee120	1	0	15704	320	50	1722	0	35	100084	79
//1.000037190421512E19	0	14102100	1005	0	1fbe01fe	f3845767	28905ebd	ecad2386	7801e8d9	07d7df22	a99f214a	b3cf8def	8a4875bd	1	0	15704	320	50	1722	0	35	100084	79
//1.0000640724480838E19	0	14102100	1005	0	1fbe01fe	f3845767	28905ebd	ecad2386	7801e8d9	07d7df22	a99f214a	e8275b8f	6332421a	1	0	15706	320	50	1722	0	35	100084	79
//1.0000679056417042E19	0	14102100	1005	1	fe8cc448	9166c161	0569f928	ecad2386	7801e8d9	07d7df22	a99f214a	9644d0bf	779d90c2	1	0	18993	320	50	2161	0	35	-1	157