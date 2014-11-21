package com.google.appengine.demos.mapreduce.entitycount;

import static java.lang.Integer.parseInt;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.security.SecureRandom;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Serves a page that allows interaction with this MapReduce application.
 *
 * Implemented for you.
 * 
 * @author aruokone
 */
@SuppressWarnings("serial")
public class ExampleServlet extends HttpServlet {

  private static final String DATASTORE_TYPE = "EE4221";

  private static final Logger log = Logger.getLogger(ExampleServlet.class.getName());

  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final SecureRandom random = new SecureRandom();
  private static URL url = null;

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String token = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + token, true);

    try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
     		
    	pw.println("<html><body>"
          + "<br><form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "Runs three MapReduces: <br /> <ul> <li> Creates MapReduce "
          + "entities of the type:  " + DATASTORE_TYPE + ".</li> "
          + "<li> Each entity presents a hotel review: entity name identifies a hotel and payload is the actual review.</li>"
          + "<li> Each hotel can have several reviews, given by different persons.</li>"
           + "<li> The MapReduce job counts the occurences of the word 'wow' for each hotel.</li>"
          + "<li> Deletes all entities of the type: " + DATASTORE_TYPE + ".</li> </ul> <div> <br />"
          + "Entities to create: <input name='entities' value='10'> <br />"
          + "Entity payload size: <input name='payloadBytesPerEntity' value='20'> <br />"
          + "Number of concurrent workers: <input name='shardCount' value='2'> <br />"
          + "Cloud Storage bucket: <input name='gcs_bucket'> (Leave empty to use the default bucket)"
          + "<br /> <input type='submit' value='Create, Count WOWs, and Delete'> <br />"
          +  "</div> </form> </body></html>");
    }
  }

  
  public static URL getFileUrl(){
  
  return url;
  }
  
  
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
	  
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    writeResponse(resp);
  }

  private String getPipelineStatusUrl(String pipelineId) {
    return "/_ah/pipeline/status.html?root=" + pipelineId;
  }

  private void redirectToPipelineStatus(HttpServletResponse resp,
      String pipelineId) throws IOException {
    String destinationUrl = getPipelineStatusUrl(pipelineId);
    log.info("Redirecting to " + destinationUrl);
    resp.sendRedirect(destinationUrl);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
	  
	  ServletContext context = req.getSession().getServletContext();
	  url = context.getResource("/WEB-INF/classes/testdata.txt");
	  
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    String token = req.getParameter("token");
    if (!memcache.delete(userService.getCurrentUser().getUserId() + " " + token)) {
      throw new RuntimeException("Bad token, try again: " + token);
    }
    String bucket = req.getParameter("gcs_bucket");
    if (Strings.isNullOrEmpty(bucket)) {
      try {
        bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
      } catch (AppIdentityServiceFailureException ex) {
        // ignore
      }
    }

    int entities = parseInt(req.getParameter("entities"));
    int bytesPerEntity = parseInt(req.getParameter("payloadBytesPerEntity"));
    int shardCount = parseInt(req.getParameter("shardCount"));
    PipelineService service = PipelineServiceFactory.newPipelineService();
    redirectToPipelineStatus(resp, service.startNewPipeline(
        new ChainedMapReduceJob(bucket, DATASTORE_TYPE, shardCount, entities, bytesPerEntity)));
  }
}
