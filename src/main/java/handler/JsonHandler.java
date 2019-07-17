package handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonSyntaxException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @function   : Flume自定义JsonHandler
 * @version    : v1.0
 * @author     : LTong
 * @createTime : 2018/12/13 11:54
 */
public class JsonHandler implements HTTPSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JsonHandler.class);

    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8")
                || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {
            LOG.error("Unsupported character set in request {}. "
                    + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.");
        }
        List<JSONEvent> eventList = new ArrayList<JSONEvent>();
        List<JSONObject> lists;
        //BufferReader转String
        StringBuffer buffer = new StringBuffer();
        String line = " ";
        while ((line = reader.readLine()) != null){
            buffer.append(line);
        }
        String jsonStr = new String(buffer.toString().getBytes(),charset);
        try {
            lists = JSON.parseObject(jsonStr.replace("\\\"", "!*!"),ArrayList.class);
            for (JSONObject list : lists) {
                JSONEvent jsonEvent = new JSONEvent();
                jsonEvent.setBody(list.getString("body").toString().getBytes());
                eventList.add(jsonEvent);
            }
        } catch (JsonSyntaxException ex) {
            throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
        }catch (JSONException e){
            System.out.println("");
        }catch (Exception e){
            System.out.println("");
        }
        for (JSONEvent e : eventList) {
            e.setCharset(charset);
        }
        return getSimpleEvents(eventList);
    }
    public void configure(Context context) {

    }

    private List<Event> getSimpleEvents(List<JSONEvent> events) {
        List<Event> newEvents = new ArrayList<Event>(events.size());
        for (JSONEvent e:events) {
            newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
        }
        return newEvents;
    }
}
