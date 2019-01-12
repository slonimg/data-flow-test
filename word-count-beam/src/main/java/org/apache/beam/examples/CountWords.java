package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class CountWords {


  static class ExtractWordsFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {

      // Split the line into words.
      String[] words = element.split("[^\\p{L}]+", -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class CountWordInstances
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }

  public static class FormatAndSortFn extends SimpleFunction<KV<String, Iterable<KV<String, Long>>>, String> {
    @Override
    public String apply(KV<String, Iterable<KV<String, Long>>> input) {
      return StreamSupport.stream(input.getValue().spliterator(), false)
          .collect((Supplier<ArrayList<KV<String, Long>>>) ArrayList::new,
              (al, kv) -> al.add(KV.of(kv.getKey(), kv.getValue())),
              (sb, kv) -> {
              })
          .stream()
          .sorted((kv1, kv2) -> {
            int res = kv2.getValue().compareTo(kv1.getValue());
            if (res == 0)
              res = kv2.getKey().compareTo(kv1.getKey());
            return res;
          })
          .collect(StringBuilder::new,
              (sb, kv) -> sb.append(String.format("%20s : %d%n", kv.getKey(), kv.getValue())),
              (sb, kv) -> {
              }).toString();
    }
  }

  public static class FormatAndSort
    extends PTransform<PCollection<KV<String, Long>>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<KV<String, Long>> counts) {

      PCollection<KV<String, Iterable<KV<String, Long>>>> single_key = counts.apply("CreateSingleKey", ParDo.of(new DoFn<KV<String, Long>, KV<String, KV<String, Long>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> element = c.element();
                        String key = element.getKey();
                        c.output(KV.of("single", KV.of(key, element.getValue())));
                    }
                }))
                .apply(GroupByKey.create());

      return single_key.apply("SortAndFormatResults", MapElements.via(new FormatAndSortFn()));
    }
  }

  public interface CountWordsOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  static void runWordCount(CountWordsOptions options) {
    Pipeline p = Pipeline.create(options);

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(new CountWordInstances())
        .apply(new FormatAndSort())
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }
  public static void main(String[] args) {
    CountWordsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CountWordsOptions.class);

    runWordCount(options);
  }
}
