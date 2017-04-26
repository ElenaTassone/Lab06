package it.polito.tdp.meteo.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import it.polito.tdp.meteo.bean.Rilevamento;

public class MeteoDAO {

	public List<Rilevamento> getAllRilevamenti() {

		final String sql = "SELECT Localita, Data, Umidita FROM situazione ORDER BY data ASC";

		List<Rilevamento> rilevamenti = new ArrayList<Rilevamento>();

		try {
			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				Rilevamento r = new Rilevamento(rs.getString("Localita"), rs.getDate("Data"), rs.getInt("Umidita"));
				rilevamenti.add(r);
			}

			conn.close();
			return rilevamenti;

		} catch (SQLException e) {

			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


	public List<Rilevamento> getAllRilevamentiLocalitaMese(int mese, String localita) {

		final String sql = "SELECT Localita, Data, Umidita FROM situazione ORDER BY data ASC";

		List<Rilevamento> rilevamentiMese = new ArrayList<Rilevamento>();

		try {
			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			//st.setString(1, localita);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				Rilevamento r = new Rilevamento(rs.getString("Localita"), rs.getDate("Data"), rs.getInt("Umidita"));
				
				//if(r.getData().after("01" String.valueOf(mese).toString() r.getData().getYear()))
					
				Calendar calendar1= Calendar.getInstance();
				Calendar calendar2= Calendar.getInstance();
				
				calendar1.setTime(r.getData());
				calendar2.set(0, mese, 0);
				
				if( calendar1.get(Calendar.MONTH)==calendar2.get(Calendar.MONTH) && r.getLocalita().compareTo(localita)==0)
					rilevamentiMese.add(r);	
			}

			conn.close();
			return rilevamentiMese ;

		} catch (SQLException e) {

			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	public Double getAvgRilevamentiLocalitaMese(int mese, String localita) {
		List<Rilevamento> rilevamentiMese = new ArrayList<Rilevamento>() ;
		rilevamentiMese = getAllRilevamentiLocalitaMese(mese, localita) ;
		Double tot = 0.0 ;
		Double avg = 0.0 ;
		for( Rilevamento r : rilevamentiMese)
			tot+=r.getUmidita();
		avg = tot/rilevamentiMese.size();
		
		avg = new BigDecimal(avg).setScale(2 , BigDecimal.ROUND_UP).doubleValue();

		return avg ;
	}

	
	/*
	 * Restistuisce la lista di tutte le citta' presenti nel DB
	 */
	public List<String> getCities() {
		final String sql = "SELECT DISTINCT localita FROM situazione";

		try {
			List<String> cities = new ArrayList<String>();

			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				cities.add(rs.getString("localita"));
			}

			conn.close();
			return cities;

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/*
	 * Dato un mese restituisce una mappa di <citta e umidita' media> 
	 */
	public Map<String, Double> getAvgRilevamentiMese(int mese) {

		final String sql = "SELECT localita, AVG(umidita) as umiditaMedia FROM situazione WHERE MONTH(Data) = ? GROUP BY localita";

		try {
			Map<String, Double> map = new TreeMap<String, Double>();

			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, mese);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				map.put(rs.getString("localita"), rs.getDouble("umiditaMedia"));
			}

			conn.close();
			return map;

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
}
