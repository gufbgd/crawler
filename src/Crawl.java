import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.xml.sax.InputSource;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

public class Crawl {

	final static Charset ENCODING = StandardCharsets.UTF_8;
	static String lstWebsites = "lstwebsites_v2.list";
	static String connectSQL = "mysql.connect";
	static String indexPath = "index";
	static int nbThreads = 300;
	static int maxCharacters = 50000;

	public static void main(String[] args) throws Exception {

		Directory indexDirectory = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new FrenchAnalyzer(Version.LUCENE_46);

		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46,
				analyzer);
		// Add new documents to an existing index:
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		IndexWriter writer = new IndexWriter(indexDirectory, iwc);

		// récupération des infos de connexion de la BD
		ArrayList<String> sMysqlConnect = LireMysqlConnect(connectSQL);
		String sqlUrl = sMysqlConnect.get(0);
		String sqlUtilisateur = sMysqlConnect.get(1);
		String sqlmotDePasse = sMysqlConnect.get(2);
		String sqlTable = sMysqlConnect.get(3);
		String sqlChampURL = sMysqlConnect.get(4);
		String sqlChampContenu = sMysqlConnect.get(5);
		String sqlChampTitre = sMysqlConnect.get(6);
		String sqlChampSummary = sMysqlConnect.get(7);
		String sqlChampPublished = sMysqlConnect.get(8);
		String sqlChampRefIDXML = sMysqlConnect.get(9);
		String sqlTableXML = sMysqlConnect.get(10);
		String sqlChampXMLID = sMysqlConnect.get(11);
		String sqlChampXMLURL = sMysqlConnect.get(12);
		String sqlChampImg = sMysqlConnect.get(13);

		Connection connexion = connectBD(sqlUrl, sqlUtilisateur, sqlmotDePasse);
		java.sql.Statement statement = null;
		ResultSet resultat = null;
		statement = connexion.createStatement();
		System.out.println("Objet requête créé !");
		
		// Exécution rq selection urls
		resultat = statement.executeQuery("SELECT " 
				+ sqlChampURL
				+ " FROM "
				+ sqlTable + " WHERE " + sqlChampContenu + " is NULL;");
		System.out.println("Requête effectuée !");

		// Récupération des données du résultat de la requête de lecture
		ArrayList<String> urlDB = new ArrayList<String>();
		while (resultat.next()) {
			urlDB.add(resultat.getString(sqlChampURL));

		}
		System.out.println(urlDB.size() + " urls sélectionnées");

		System.out.println("Fermeture de l'objet ResultSet.");
		if (resultat != null) {
			try {
				resultat.close();
			} catch (SQLException ignore) {
			}
		}
		System.out.println("Fermeture de l'objet Statement.");
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException ignore) {
			}
		}

		// List<String> urlDB = readTextFile(lstWebsites);
		Collections.shuffle(urlDB);
		ArrayList<String> crawltext = new ArrayList<String>();

		int packets = (int) Math.ceil((double) urlDB.size() / nbThreads);
		int reste = urlDB.size() % nbThreads;

		for (int i = 0; i < packets; i++) {
			ArrayList<CrawlArticle> lst = new ArrayList<CrawlArticle>();
			for (int k = 0; k < nbThreads; k++) {
				if (i > (packets - 2) & k > (reste - 1)) {
					break;
				}
				String s = urlDB.get(i * nbThreads + k);
				CrawlArticle a = new CrawlArticle(s);
				lst.add(a);
				a.start();
				System.out.println("Lancement crawl de " + s + "...");

			}
			try {
				for (CrawlArticle a : lst) {
					a.join(3000);
				}
			} catch (InterruptedException IntExp) {
			}
			for (CrawlArticle a : lst) {
				String text = a.text;
				if (text.length() > maxCharacters) {					
					text = text.substring(0, maxCharacters);
				}
				String siteWeb = a.urltxt;
				String img = a.urlimg;

				System.out.println(siteWeb);

				if (text != "") {
					indexDoc(writer, siteWeb, text);
					System.out.println("Index Lucene mis à jour");
				}
				
				//test si champ img à mettre à jour
				
				/*ResultSet resChampImg = null; 
				statement = connexion.createStatement();
				resChampImg = statement.executeQuery("SELECT " 
						+ sqlChampImg + " FROM "
						+ sqlTable + " WHERE " + sqlChampURL + "='" + siteWeb + "';");
				*/
				// update de la BD
				// un ou 2 champs
				if(checkIfNull(connexion, sqlTable,
						sqlChampURL, sqlChampImg , siteWeb)){
					String query = "UPDATE " + sqlTable + " SET " 
							+ sqlChampContenu
							+ " = (?)"
							+ "," + sqlChampImg
							+ " = (?)"
							+ " WHERE "
							+ sqlChampURL + "='" + siteWeb + "';";
					PreparedStatement pstatement = connexion
							.prepareStatement(query);
					System.out.println(query);
					ByteArrayInputStream bais = new ByteArrayInputStream(
							text.getBytes());
					pstatement.setBinaryStream(1, bais, text.getBytes().length);
					if(img.equals("")){
						pstatement.setNull(2, Types.VARCHAR);;	
					} else {
						pstatement.setString(2, img);
					}
					pstatement.executeUpdate();
					pstatement.close();
				} else {
					String query = "UPDATE " + sqlTable + " SET " 
							+ sqlChampContenu
							+ " = (?)"
							+ " WHERE "
							+ sqlChampURL + "='" + siteWeb + "';";
					PreparedStatement pstatement = connexion
							.prepareStatement(query);
					System.out.println(query);
					ByteArrayInputStream bais = new ByteArrayInputStream(
							text.getBytes());
					pstatement.setBinaryStream(1, bais, text.getBytes().length);	
					pstatement.executeUpdate();
					pstatement.close();
				}
				statement.close();
				//fin

			}

		}

		/*
		 * for (String siteWeb : urlDB) { String text;
		 * 
		 * 
		 * text = retrieveSiteweb(siteWeb);
		 * 
		 * 
		 * System.out.println(text); if(text!=""){ indexDoc(writer, siteWeb,
		 * text); } // update de la BD String query = "UPDATE " + sqlTable +
		 * " SET " + sqlChampContenu + "= (?) WHERE " + sqlChampURL + "='" +
		 * siteWeb + "';"; PreparedStatement pstatement =
		 * connexion.prepareStatement(query); System.out.println(query);
		 * ByteArrayInputStream bais = new
		 * ByteArrayInputStream(text.getBytes()); pstatement.setBinaryStream(1,
		 * bais, text.getBytes().length); pstatement.executeUpdate();
		 * 
		 * pstatement.close();
		 * 
		 * }
		 */
		writer.close();

		System.out.println("Fermeture de l'objet Connection.");
		if (connexion != null) {
			try {
				connexion.close();
			} catch (SQLException ignore) {
			}
		}

		/* lecture */

		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(
				indexPath)));

		//indexLecture(reader);

		// recherche
		/*
		 * 
		 * IndexSearcher searcher = new IndexSearcher(reader); // Term t = new
		 * Term(ARTICLETEXT, "wembley");
		 * 
		 * PhraseQuery query = new PhraseQuery(); // MultiPhraseQuery query =
		 * new MultiPhraseQuery();
		 * 
		 * Terms terms = reader.getTermVector(0, ARTICLETEXT); if (terms !=
		 * null) { TermsEnum term = null; term = terms.iterator(term); while
		 * (term.next() != null) { Term t = new Term(ARTICLETEXT,
		 * term.toString()); query.add(t); } } else { System.err.println("doc "
		 * + 0 + " - aucun"); }
		 * 
		 * // Query query = new TermQuery(t); TopDocs results =
		 * searcher.search(query, 100); ScoreDoc[] hits = results.scoreDocs;
		 * 
		 * for (int i = 0; i < 3; i++) { System.out.println(hits[i].score); }
		 */

		reader.close();

	}

	static String retrieveSiteweb(String siteWeb) {
		// gestion exceptions lors récupération urls
		URL url = null;
		String text = "";
		try {
			url = new URL(siteWeb);
		} catch (MalformedURLException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}
		InputSource is = new InputSource();
		is.setEncoding("UTF-8");
		try {
			is.setByteStream(url.openStream());
		} catch (IOException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}
		try {
			text = ArticleExtractor.INSTANCE.getText(is);
		} catch (BoilerpipeProcessingException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}

		return text;
	}

	static List<String> readTextFile(String aFileName) throws IOException {
		Path path = Paths.get(aFileName);
		return Files.readAllLines(path, ENCODING);
	}

	/* Indexed, tokenized, stored. */
	public static final String ARTICLETEXT = "ArticleText";
	public static final FieldType TYPE_STORED = new FieldType();

	static {
		TYPE_STORED.setIndexed(true);
		TYPE_STORED
				.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		TYPE_STORED.setTokenized(true);
		TYPE_STORED.setStored(true);
		TYPE_STORED.setStoreTermVectors(true);
		TYPE_STORED.setStoreTermVectorPositions(true);
		// TYPE_STORED.freeze();
	}

	static void indexDoc(IndexWriter writer, String articleName,
			String articleText) throws IOException {

		Document doc = new Document();
		doc.add(new StringField("name", articleName, Field.Store.YES));
		doc.add(new Field(ARTICLETEXT, articleText, TYPE_STORED));

		writer.addDocument(doc);

		System.out.println("Indexé " + articleName);
		// System.out.println(articleText);

	}

	static void indexLecture(IndexReader reader) throws IOException {
		int max = reader.maxDoc();
		System.out.println(max);
		TermsEnum term = null;
		// iterate docs
		for (int i = 0; i < max; ++i) {
			// get term vector for body field
			final Terms terms = reader.getTermVector(i, ARTICLETEXT);
			if (terms != null) {
				// count terms in doc
				int numTerms = 0;
				term = terms.iterator(term);
				while (term.next() != null) {
					System.out.println("doc " + i + " - term '"
							+ term.term().utf8ToString() + "' "
							+ term.totalTermFreq());
					++numTerms;
				}
				System.out.println("doc " + i + " - " + numTerms + " terms");
			} else {
				System.err.println("doc " + i + " - aucun");
			}
		}
	}

	public static Connection connectBD(String url, String utilisateur,
			String motDePasse) {

		/* Chargement du driver JDBC pour MySQL */
		try {
			// messages.add( "Chargement du driver..." );
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Driver chargé !");
		} catch (ClassNotFoundException e) {
			System.out
					.println("Erreur lors du chargement : le driver n'a pas été trouvé dans le classpath ! <br/>"
							+ e.getMessage());
		}

		/* Connexion à la base de données */

		Connection connexion = null;

		try {
			System.out.println("Connexion à la base de données...");
			connexion = (Connection) DriverManager.getConnection(url,
					utilisateur, motDePasse);
			System.out.println("Connexion réussie !");

			/* Création de l'objet gérant les requêtes */
		} catch (SQLException e) {
			System.out.println("Erreur lors de la connexion : <br/>"
					+ e.getMessage());
		}
		return connexion;

	}

	static ArrayList<String> LireMysqlConnect(String filename)
			throws IOException {
		List<String> lines = readTextFile(filename);
		ArrayList<String> res = new ArrayList<String>();
		for (String s : lines) {
			String part[] = s.split("::");
			if (part.length > 1) {
				res.add(part[1]);
			} else {
				res.add("");
			}

		}
		// System.out.println(res.get(0));

		return res;

	}
	
	static boolean checkIfNull(Connection connexion, String table,
			String champ, String champ2 , String link) throws SQLException {

		java.sql.Statement statement = null;
		ResultSet resultat = null;
		statement = connexion.createStatement();
		System.out.println("Objet requête créé !");
		// Exécution d'une requête de lecture
		resultat = statement.executeQuery("SELECT " + champ + " FROM " + table
				+ " WHERE " 
				+ champ + "='" + link + "' and "
				+ champ2 + " is null"
				+";");
		if (resultat.last()) {
			System.out.println("Img update " + link);
			return true;
		} else {
			System.out.println("****** No Img update ");
			return false;
		}
	}
	

}
