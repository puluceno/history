package ca.history.main;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.set;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import org.bson.Document;
import org.pmw.tinylog.Logger;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlInput;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;
import com.gargoylesoftware.htmlunit.html.HtmlTable;
import com.gargoylesoftware.htmlunit.html.HtmlTableCell;
import com.gargoylesoftware.htmlunit.html.HtmlTextInput;
import com.mongodb.client.MongoCollection;

public class CAHistoryDownloader extends Thread {

	private static MongoCollection<Document> caCollection = MongoResource.getDataBase("ca").getCollection("ca");

	private static AtomicInteger number = new AtomicInteger(0);
	private static Object[] updateList;

	public static void main(String[] args) throws Exception {
		crawlCAS();
	}

	public static void crawlCAS() throws Exception {
		int threads = 1;
		Logger.info("Procedure started. Running with {} threads", threads);

		updateList = caCollection.find(and(exists("history", false), exists("number", true)))
				.projection(fields(include("number"))).sort(ascending("number")).into(new ArrayList<Document>())
				.toArray();

		Logger.info("Updating {} CA...", updateList.length);

		List<CAHistoryDownloader> list = new ArrayList<CAHistoryDownloader>();

		for (int i = 0; i < threads; i++) {
			CAHistoryDownloader thread = new CAHistoryDownloader();
			list.add(thread);
		}
		for (CAHistoryDownloader a : list) {
			a.start();
			Thread.sleep(1000);
		}

	}

	@Override
	public void run() {
		while (number.get() < updateList.length) {
			String caNumber = "";
			synchronized (updateList) {
				caNumber = ((Document) updateList[number.getAndIncrement()]).getString("number");
			}

			WebClient webClient = initializeClient();
			Logger.info("Updating CA {}", caNumber);
			long beginCA = new Date().getTime();

			try {
				URL url = new URL("http://caepi.mte.gov.br/internet/ConsultaCAInternet.aspx");

				HtmlPage page = (HtmlPage) webClient.getPage(url);

				HtmlTextInput inputNumber = (HtmlTextInput) page.getElementById("txtNumeroCA");
				inputNumber.setValueAttribute(caNumber);

				HtmlSubmitInput search = (HtmlSubmitInput) page.getElementById("btnConsultar");
				HtmlPage page2 = search.click();

				HtmlInput details = null;
				int tries = 40;
				String xpath = ".//td[contains(.,'" + caNumber + "')]/following-sibling::td[4]/input";
				while (tries > 0 && details == null) {
					tries--;
					details = (HtmlInput) page2.getFirstByXPath(xpath);
					synchronized (page2) {
						page2.wait(1500);
					}
				}
				HtmlPage page3 = details.click();

				HtmlTable table = null;
				int tries2 = 40;
				while (tries2 > 0 && table == null) {
					tries2--;
					table = (HtmlTable) page3.getElementById("PlaceHolderConteudo_grdListaHistoricoAlteracao");
					synchronized (page2) {
						page3.wait(1500);
					}
				}

				if (table != null) {
					List<Document> history = new ArrayList<Document>();
					for (int i = 1; i < table.getRowCount(); i++) {
						for (final HtmlTableCell cell : table.getRow(i).getCells()) {
							if (cell.getIndex() == 1)
								history.add(new Document("date", cell.asText()));
							if (cell.getIndex() == 2)
								history.get(i - 1).append("status", cell.asText());
						}

					}
					Logger.info("CA {} updated: {}", caNumber,
							caCollection.updateMany(eq("number", caNumber), set("history", history)).wasAcknowledged());
				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				webClient.getCurrentWindow().getJobManager().removeAllJobs();
				webClient.close();
				Logger.info("Total time: " + (new Date().getTime() - beginCA));
			}
		}
		Logger.info("Operarion finished, there are no CA's left in the list");
	}

	private static WebClient initializeClient() {
		final WebClient webClient = new WebClient();
		webClient.getOptions().setHistorySizeLimit(1);
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setPrintContentOnFailingStatusCode(false);
		webClient.getOptions().setUseInsecureSSL(true);
		webClient.getOptions().setDoNotTrackEnabled(true);
		java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(Level.OFF);
		java.util.logging.Logger.getLogger("org.apache.http.client.protocol.ResponseProcessCookies")
				.setLevel(Level.OFF);
		return webClient;
	}
}