/**
 * 
 */
/**
 * @author user
 *
 */
package hive;


/**
 * 
 */
/**
 * @author qiaopengfei
 *
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;








public class hive_jingweidu_2 extends UDF{
	
	public String evaluate(String name){
		 if (name.length()==0){
			 return ""+","+""; 
		 } 
		 else if(name.trim().length()==0){
			 return ""+","+""; 
		 }
		
		 
		{
			if(name.contains("¡¢")){
			String new_name=name.split("¡¢")	[0];
			
			Map<String, String> json = getGeocoderLatitude(new_name);
			String lng1 = json.get("lng");
			String lat1 = json.get("lat");
			return lat1 + "," + lng1;
		 }
			else {
				Map<String, String> json = getGeocoderLatitude(name);
				String lng1 = json.get("lng");
				String lat1 = json.get("lat");
				return lat1 + "," + lng1;
			}
		}
	
	}

	public static Map<String, String> getGeocoderLatitude(String address) {
		BufferedReader in = null;
		try {
			Map paramsMap = new LinkedHashMap<String, String>();
			paramsMap.put("address", address);
			paramsMap.put("output", "json");
			paramsMap.put("ak", "vrvU8CB1E8AqaDxnGaYUFCoT02gylwzg");
			String quest = GetLatitude.toQueryString(paramsMap);
			URL tirc = new URL(
					"http://api.map.baidu.com/geocoder/v2/?" + quest + "&sn=" + GetLatitude.result(paramsMap));
			System.out.println(tirc);
			in = new BufferedReader(new InputStreamReader(tirc.openStream(), "UTF-8"));
			String res;
			StringBuilder sb = new StringBuilder("");
			while ((res = in.readLine()) != null) {
				sb.append(res.trim());
			}
			String str = sb.toString();
			Map<String, String> map = null;
			if (StringUtils.isNotEmpty(str)) {
				if (str.contains("lng")) {
					//System.out.println("haaaaaaaaa");
					// {"status":0,"result":{"location":{"lng":104.62736741822471,"lat":26.61732503419995},"precise":0,"confidence":30,"comprehension":32,"level":"´å×¯"}}
					int lngStart = str.indexOf("lng\":");
					int lngEnd = str.indexOf(",\"lat");
					int latEnd = str.indexOf("},\"precise");

					int statusstart = str.indexOf("status\":");
					int statusEnd = str.indexOf(",\"result");
					// System.out.println(status);
					if (lngStart > 0 && lngEnd > 0 && latEnd > 0 && statusstart > 0 && statusEnd > 0) {
						String lng = str.substring(lngStart + 5, lngEnd);
						String lat = str.substring(lngEnd + 7, latEnd);

						String status = str.substring(statusstart + 8, statusEnd);
						System.out.println(status);

						map = new HashMap<String, String>();
						System.out.println("second");
						map.put("lng", lng);
						map.put("lat", lat);
						// map.put("status", status);
						return map;
					}

				} else {
					map = new HashMap<String, String>();

					map.put("lng", "null");
					map.put("lat", "null");

					return map;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
 
}