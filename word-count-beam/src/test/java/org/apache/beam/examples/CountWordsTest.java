package org.apache.beam.examples;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

import static org.apache.beam.examples.WordCountTest.COUNTS_ARRAY;

@RunWith(JUnit4.class)
public class CountWordsTest {

  @Test
  public void testExtractWordsFn() throws Exception {
    DoFnTester<String, String> extractWordsFn = DoFnTester.of(new CountWords.ExtractWordsFn());

    Assert.assertThat(
        extractWordsFn.processBundle(" some  input  words "),
        CoreMatchers.hasItems("some", "input", "words"));
    Assert.assertThat(extractWordsFn.processBundle(" "), CoreMatchers.hasItems());
    Assert.assertThat(
        extractWordsFn.processBundle(" some ", " input", " words"),
        CoreMatchers.hasItems("some", "input", "words"));
  }

  @Rule
  public TestPipeline p = TestPipeline.create();


  static final String[] WORDS_ARRAY =
      new String[] {
          "hi there", "hi", "hi sue bob",
          "hi sue", "", "bob hi"
      };

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Test
  @Category(ValidatesRunner.class)
  public void testCountWords() throws Exception {
    PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(new CountWords.CountWordInstances()).apply(MapElements.via(new CountWords.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run().waitUntilFinish();
  }
}
