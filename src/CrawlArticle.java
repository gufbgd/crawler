import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLFetcher;
import de.l3s.boilerpipe.sax.HTMLHighlighter;

public class CrawlArticle extends Thread {
	public String urltxt;
	public String text;
	public String urlimg;

	public CrawlArticle(String url) {
		this.urltxt = url;
	}

	public void run() {
		URL url = null;
		text = "";
		urlimg = "";

		try {
			url = new URL(urltxt);
		} catch (MalformedURLException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}
		InputSource is = new InputSource();
		is.setEncoding("UTF-8");

        //InputSource is;
        
		/*try {
			is = HTMLFetcher.fetch(url).toInputSource();
			BoilerpipeSAXInput in;
			in = new BoilerpipeSAXInput(is);
			final TextDocument doc = in.getTextDocument();
			text = ArticleExtractor.INSTANCE.getText(doc);
		} catch (IOException | SAXException | BoilerpipeProcessingException e1) {
			// TODO Bloc catch généré automatiquement
			e1.printStackTrace();
		}*/
       
        

		try {
			is.setByteStream(url.openStream());
		} catch (IOException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}
		try {
			final BoilerpipeExtractor extractor = CommonExtractors.ARTICLE_EXTRACTOR;
			/*final HTMLHighlighter hh = HTMLHighlighter.newExtractingInstance();
			try {
				text = hh.process(url, extractor);
			} catch (IOException e1) {
				// TODO Bloc catch généré automatiquement
				e1.printStackTrace();
			} catch (SAXException e1) {
				// TODO Bloc catch généré automatiquement
				e1.printStackTrace();
			}*/
			//is.setEncoding("UTF-8");
			//text = ArticleExtractor.INSTANCE.getText(is);
			text = ArticleExtractor.INSTANCE.getText(url);
			System.out.println(text);
			final ImageExtractor ie = ImageExtractor.INSTANCE;
			List<Image> imgUrls;
			try {
				imgUrls = ie.process(url, extractor);
				Collections.sort(imgUrls);
				if (!imgUrls.isEmpty()) {
					urlimg = imgUrls.get(0).getSrc();
				}
				/*
				 * for(Image img : imgUrls) {
				 * System.out.println("* "+img.getSrc()); }
				 */
			} catch (IOException | SAXException e) {
				// TODO Bloc catch généré automatiquement
				e.printStackTrace();
			}

		} catch (BoilerpipeProcessingException e) {
			// TODO Bloc catch généré automatiquement
			e.printStackTrace();
		}

	}

}
