package it.polito.tdp.meteo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import it.polito.tdp.meteo.bean.Citta;
import it.polito.tdp.meteo.bean.SimpleCity;
import it.polito.tdp.meteo.db.MeteoDAO;

public class Model {

	public enum Months {

		JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC;

		private static Map<Integer, Months> ss = new TreeMap<Integer, Months>();
		private static final int START_VALUE = 1;
		private int value;

		static {
			for (int i = 0; i < values().length; i++) {
				values()[i].value = START_VALUE + i;
				ss.put(values()[i].value, values()[i]);
			}
		}

		public static Months fromInt(int i) {
			return ss.get(i - 1);
		}

		public int value() {
			return value;
		}
	}

	private final static int COSTO_CAMBIO = 100;
	private final static int NUMERO_GIORNI_CITTA_CONSECUTIVI_MIN = 3;
	private final static int NUMERO_GIORNI_CITTA_MAX = 6;
	private final static int NUMERO_GIORNI_TOTALI = 15;

	MeteoDAO meteoDAO = null;
	private ArrayList<Citta> cities = null;

	private double punteggioMiglioreSoluzione;
	private ArrayList<SimpleCity> miglioreSoluzione = null;

//public Model() {
//		
//		listaRilevamenti = dao.getAllRilevamenti();
//		
//		for (Rilevamento r : listaRilevamenti ){
//			Citta citta = new Citta(r.getLocalita()) ;
//			if(!listaCitta.contains(citta)){
//				listaCitta.add(citta);
//			}
//		}
//		
//		for(Citta c : listaCitta){
//			List<Rilevamento> listaRilevamentiPerCitta = new ArrayList<Rilevamento> () ;
//			for(Rilevamento r : listaRilevamenti){
//				if(r.getLocalita().compareTo(c.toString())==0)
//					listaRilevamentiPerCitta.add(r) ;
//					
//				}
//			c.setRilevamenti(listaRilevamentiPerCitta) ;
//			}
//		}
//
//	public String getUmiditaMedia(int mese) {
//		String result = "" ;
//		for(Citta c : listaCitta){
//			double avg = dao.getAvgRilevamentiLocalitaMese(mese, c.toString()) ;
//			result += c.toString()+ " media umidita: " + avg + "\n"; 
//		}
//
//		return result;
//	}

	public Model() {
		meteoDAO = new MeteoDAO();

		cities = new ArrayList<Citta>();
		for (String s : meteoDAO.getCities())
			cities.add(new Citta(s));
	}

	public String getUmiditaMedia(int mese) {

		if (mese < 1 || mese > 12)
			return "**ERRORE** Mese deve essere nel range 1-12\n";

		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Umidita' media nel mese di %s: \n", Months.values()[mese - 1]));

		Map<String, Double> avgHum = meteoDAO.getAvgRilevamentiMese(mese);
		for (String s : avgHum.keySet()) {
			sb.append(String.format("localita: %s \tumidita': %f \n", s, avgHum.get(s)));
		}

		return sb.toString();
	}

	private void resetCities(int mese) {
		for (Citta c : cities) {
			c.setCounter(0);
			c.setRilevamenti(meteoDAO.getAllRilevamentiLocalitaMese(mese, c.getNome()));
		}
	}

	public String trovaSequenza(int mese) {

		if (mese < 1 || mese > 12)
			return "**ERRORE** Mese deve essere nel range 1-12\n";

		punteggioMiglioreSoluzione = Double.MAX_VALUE;
		miglioreSoluzione = null;

		this.resetCities(mese);

		System.out.println("START");

		recursive(new ArrayList<SimpleCity>(), 0);

		System.out.println("FINISH!");

		if (miglioreSoluzione != null) {
			System.out.println(String.format("Soluzione miglioer per il mese di %s: \n", Months.values()[mese - 1]));
			System.out.println(String.format("DEBUG score: %f", this.punteggioSoluzione(miglioreSoluzione)));
			return miglioreSoluzione.toString();
		}

		return "Nessuna soluzione trovata";
	}

	private Double punteggioSoluzione(List<SimpleCity> soluzioneCandidata) {

		// Controllo che la lista non sia nulla o vuota
		if (soluzioneCandidata == null || soluzioneCandidata.size() == 0)
			return Double.MAX_VALUE;

		// Controllo che la soluzione contenga tutte le citta'
		for (Citta c : cities) {
			if (!soluzioneCandidata.contains(new SimpleCity(c.getNome())))
				return Double.MAX_VALUE;
		}

		SimpleCity previous = soluzioneCandidata.get(0);
		double score = 0.0;

		for (SimpleCity sc : soluzioneCandidata) {
			if (!previous.equals(sc)) {
				score += COSTO_CAMBIO;
			}
			previous = sc;
			score += sc.getCosto();
		}

		return score;
	}

	private boolean controllaParziale(List<SimpleCity> parziale) {

		// Se e' nulla non e' valida
		if (parziale == null)
			return false;

		// Se la soluzione parziale e' vuota, e' valida
		if (parziale.size() == 0)
			return true;

		// Controllo sui vincoli del numero di giorni massimo in ciascuna citta
		for (Citta citta : cities) {
			if (citta.getCounter() > NUMERO_GIORNI_CITTA_MAX)
				return false;
		}

		// Controllo sul vincolo del numero minimo di giorni consecutivi
		SimpleCity previous = parziale.get(0);
		int counter = 0;

		for (SimpleCity sc : parziale) {
			if (!previous.equals(sc)) {
				if (counter < NUMERO_GIORNI_CITTA_CONSECUTIVI_MIN) {
					return false;
				}
				counter = 1;
				previous = sc;
			} else {
				counter++;
			}
		}

		return true;
	}

	private void recursive(List<SimpleCity> parziale, int step) {

		// condizione di terminazione
		if (step >= NUMERO_GIORNI_TOTALI) {

			double score = this.punteggioSoluzione(parziale);

			if (score < punteggioMiglioreSoluzione) {

				// System.out.println(parziale.toString());
				// System.out.println(String.format("DEBUG score: %f", score));

				punteggioMiglioreSoluzione = score;
				miglioreSoluzione = new ArrayList<SimpleCity>(parziale);
			}
			return;
		}

		for (Citta citta : cities) {

			SimpleCity sc = new SimpleCity(citta.getNome(), citta.getRilevamenti().get(step).getUmidita());

			parziale.add(sc);
			citta.increaseCounter();

			if (controllaParziale(parziale))
				recursive(parziale, step + 1);

			parziale.remove(step);
			citta.decreaseCounter();
		}
	}
}