package com.knossys.rnd.data;

import org.apache.log4j.Logger;

import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;
import java.io.*;

import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

/**
 * 
 */
public class KDataConnectorJFX extends Application {

  private static Logger M_log = Logger.getLogger(KDataConnectorJFX.class);
  
  private WatchService watcher=null;
  private Path dir=null;
  
  private MySQLDriver connector=null;
  
  /**
   *  
   */
  private void usage () {
    M_log.info("usage()");
    
  }
  
  /**
   *  
   */
  public Boolean init (String[] args) {
    M_log.info("init()");
    
    if (args.length < 1) {
      usage();
      return (false);
    }

    dir = null;
    
    for (int i=0;i<args.length;i++) {
      String anArg=args [i];
      if (anArg.equalsIgnoreCase("-d")==true) {
        dir = Paths.get(args [i+1]);
      }
    }
    
    if (dir==null) {
      M_log.info("Error: no path provided");
      usage();
      return (false);
    }    
    
    if (Files.exists(dir)==false) {
      M_log.info("Error: directory does not exist: " + dir.getFileName());
      return (false);
    }
    
    try {
      this.watcher = FileSystems.getDefault().newWatchService();
    } catch (IOException e) {
      M_log.info(e.getMessage());
    }
    try {
      dir.register(watcher, ENTRY_CREATE);
    } catch (IOException e) {
      M_log.info(e.getMessage()); 
    }
    
    connector=new MySQLDriver ();
    connector.init();
    
    return (true);
  }

  /**
   * Process all events for the key queued to the watcher.
   */
  void monitor() {
    M_log.info("monitor()");
    
    for (;;) {
      // wait for key to be signaled
      WatchKey key;
      try {
        key = watcher.take();
      } catch (InterruptedException x) {
        return;
      }

      for (WatchEvent<?> event: key.pollEvents()) {
        WatchEvent.Kind kind = event.kind();

        if (kind == OVERFLOW) {
          continue;
        }

        // The filename is the context of the event.
        WatchEvent<Path> ev = (WatchEvent<Path>)event;        
        Path filename = ev.context();

        /*
        // Verify that the new file is a text file. Can't do binary files yet anyway
        try {
          Path child = dir.resolve(filename);
          if (!Files.probeContentType(child).equals("text/plain")) {
            System.err.format("New file '%s' is not a plain text file.%n", filename);
            continue;
          }
        } catch (IOException x) {
          System.err.println(x);
          continue;
        }
        */

        processFile (filename);
      }

      //Reset the key -- this step is critical if you want to receive
      //further watch events. If the key is no longer valid, the directory
      //is inaccessible so exit the loop.
      boolean valid = key.reset();
      if (!valid) {
        break;
      }
    }
  }

  /** 
   * @param filename
   */
  private void processFile(Path filename) {
    M_log.info("processFile ("+filename+")");
    
    String fileString=filename.toString();
    
    if ((fileString.contains(".csv")==true) || (fileString.contains(".CSV")==true)) {
      processCSV (dir.getFileName() + "/" + fileString);
      return;
    }
    
    if ((fileString.contains(".xlsx")==true) || (fileString.contains(".XLSX")==true)) {
      processXLSX (dir.getFileName() + "/" + fileString);
      return;      
    }
    
    M_log.info("No valid processor found");
  }

  /** 
   * @param string
   */
  private void processXLSX(String filename) {
    M_log.info("processXLSX ("+filename+")");
    
  }

  /** 
   * @param string
   */
  private void processCSV(String filename) {
    M_log.info("processCSV("+filename+")");
    
  }

  @Override
  public void start(Stage primaryStage) throws Exception {
    M_log.info("start()");
    
    final TextField nameText = new TextField();
    nameText.setText("Stef");
    nameText.setLayoutX(10);
    nameText.setLayoutY(10);

    final Button button = new Button();
    button.setLayoutX(160);
    button.setLayoutY(10);
    button.setText("Greet me!");

    final Label greetingLabel = new Label();
    greetingLabel.setLayoutX(10);
    greetingLabel.setLayoutY(40);

    button.setOnAction(event -> greetingLabel.setText("Hello " + nameText.getText() + "!"));

    final Group root = new Group();
    root.getChildren().addAll(nameText, button, greetingLabel);

    primaryStage.setTitle("Hello World");
    primaryStage.setScene(new Scene(root, 240, 70));
    primaryStage.show();    
  }  
  
  /** 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    KDataConnectorJFX connector=new KDataConnectorJFX();
    if (connector.init(args)==true) {
      connector.monitor();
    }
  }
}
