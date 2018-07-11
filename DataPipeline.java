/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gfs.data.pipeline;

import java.sql.PreparedStatement;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gfs.data.pipeline.jdbc.RowToEntityMapper;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;

/**
 * A starter example for writing Beam programs.
 * <p>
 * The example takes two strings, converts them to their upper-case representation and logs them.
 * <p>
 * To run this starter example locally using DirectRunner, just execute it without any additional
 * parameters from your favorite development environment.
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform, you should specify
 * the following command-line options: --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */

// Future references: Creating parameter file in GCS
// https://stackoverflow.com/questions/47506355/apache-beam-options-from-property-file
// https://stackoverflow.com/questions/46676947/google-cloud-dataflow-access-txt-file-on-cloud-
// storage https://cloud.google.com/shell/docs/starting-cloud-shell
//
// Using side inputs/outputs
// http://www.waitingforcode.com/apache-beam/side-output-apache-beam/read
// http://meethassan.net/2017/12/17/unit-testing-a-dataflow-apache-beam-pipeline-that-takes-a-side-input/
// https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/FilterExamples.java
// https://stackoverflow.com/questions/45066244/multiple-cogroupbykey-with-same-key-apache-beam
// https://beam.apache.org/blog/2017/02/13/stateful-processing.html
//
// DB queries
// https://stackoverflow.com/questions/47086814/beam-dataflow-design-pattern-to-enrich-documents-based-on-database-queries?rq=1
// https://stackoverflow.com/questions/44699643/connecting-to-cloud-sql-from-dataflow-job
//
// General/misc
// https://cloud.google.com/sql/docs/postgres/connect-external-app
// https://index.scala-lang.org/spotify/dbeam/dbeam-core/0.3.9?target=_2.12
// https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/src/main/java/com/google/cloud/teleport/templates/TextToDatastore.java
//
// Performance
// https://stackoverflow.com/questions/48387097/datastore-poor-performance-with-apache-beam-dataflow?rq=1
// https://stackoverflow.com/questions/29147057/how-to-output-all-values-from-a-key-value-pair-grouped-by-key-using-google-d?noredirect=1&lq=1
// https://stackoverflow.com/questions/47162365/best-way-to-prevent-fusion-in-google-dataflow
//
// Apache Beam
// https://beam.apache.org/documentation/sdks/javadoc/2.5.0/
//
// Postgres
// https://www.postgresql.org/docs/9.6/static/index.html
//
// Templates
// https://cloud.google.com/dataflow/docs/templates/creating-templates#creating-and-staging-templates
// https://cloud.google.com/dataflow/docs/templates/provided-templates

public class DataPipeline {
    static final Logger LOG = LoggerFactory.getLogger(DataPipeline.class);

    public static void main(final String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
        final MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        // these are very important as they configure the proper gcp networks to
        // run in (GFS GCP projs have no default)
        options.setNetwork("net-prd-data-landing");
        options.setSubnetwork("regions/us-central1/subnetworks/net-data-landing-workloads");

        final Pipeline p = Pipeline.create(options);

        // TODO: parameterize DB and instance names
        final String databaseName = "master_data_staging";
        final String instanceConnectName = "gcp-gfs-data-landing-prd:us-central1:gcp-gfs-data-staging-master-db-pg-ents";
        final String jdbcUrl = String.format(
            "jdbc:postgresql://google/%s?socketFactory=com.google.cloud.sql.postgres.SocketFactory&socketFactoryArg=%s",
            databaseName,
            instanceConnectName);

        LOG.info("JDBC URL: " + jdbcUrl);
        // TODO: parameterize the Kind
        final IncompleteKey partialKey = IncompleteKey.newBuilder(options.getProject(), "Customer").build();

        // String.format("jdbc:postgresql://google/%s?socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        // + "&socketFactoryArg=%s", args)
        final PTransform<PBegin, PCollection<Entity>> jdbcData = JdbcIO.<Entity> read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver",
                jdbcUrl).withUsername("exbvm").withPassword("exbvm"))
            .withQuery(QUERY + " where cu.gfs_customer_id= ?") // '100010148'
            // .withCoder(MapCoder.of(StringUtf8Coder.of(), NullableCoder.of(StringUtf8Coder.of())))
            .withCoder(SerializableCoder.of(Entity.class))
            .withStatementPreparator(new JdbcIO.StatementPreparator() {
                @Override
                public void setParameters(final PreparedStatement preparedStatement) throws Exception {
                    preparedStatement.setString(1, "100019539");
                }
            })
            // TODO: parameterize the key column(s)
            .withRowMapper(new RowToEntityMapper("customer_id", partialKey));

        // p.apply(kafkaData).apply(Values.<byte[]> create()) //
        final PCollection<Entity> sqlEntities = p.apply("ReadFromDatabase", jdbcData);

        sqlEntities.apply("WriteToDatastore", DatastoreIO.v1().write().withProjectId(options.getProject()));

        // sqlEntities.apply(ParDo.of(new DoFn<Entity, Void>() {
        //
        // @ProcessElement
        // public void processElement(final ProcessContext c) {
        // final Entity row = c.element();
        // LOG.info("Entity key='{}'; Entity properties:", row.getKey());
        // for (final String entry : row.getNames()) {
        // LOG.info("{}='{}'", entry, row.getValue(entry));
        // }
        //
        // }
        // }));

        // .apply(MapElements.via(new SimpleFunction<String, Object>() {
        // @Override
        // public Object apply(final String input) {
        // return super.apply(input);
        // }
        // // @Override
        // // public String apply(final String input) {
        // // return input.toUpperCase();
        // // }
        // }))

        p.run();

    }

    // +++++++++++++++++++ Doesn't compile..... WIP
    // static class EntityConverter extends SimpleFunction<Entity, com.google.datastore.v1.Entity> {
    // @ProcessElement
    // public void processElement(final ProcessContext c) {
    // }
    //
    // // Contents of this method are borrowed from the
    // // com.google.cloud.datastore.BaseEntity class, then modified slightly since the method
    // // accesses
    // // internal entity properties we can't access here.
    // // Can't invoke the existing method as it is not public. Also see:
    // // https://stackoverflow.com/a/40728926/944849
    // com.google.datastore.v1.Entity toPb(final Entity dsEntity) {
    // final com.google.datastore.v1.Entity.Builder entityPb = com.google.datastore.v1.Entity
    // .newBuilder();
    // for (final String entry : dsEntity.getNames()) {
    // entityPb.putProperties(entry, DatastoreHelper.makeValue(dsEntity.getValue(entry)).build());
    // }
    // if (dsEntity.getKey() != null) {
    // entityPb.setKey(key.toPb());
    // }
    // return entityPb.build();
    // }
    // }

    static void printBytes(final byte[] byteArray) {
        for (final byte signedByte : byteArray) {
            final int unsignedInt = signedByte & 0xFF;
            System.out.println(String.format("Dec: %4d Hex: %02x Chr: '%c'",
                unsignedInt,
                unsignedInt,
                unsignedInt));
        }

    }

    // TODO: this will come in as an option for reuse, and likely will be
    // referencing a well-defined
    // view
    static String QUERY = "select "
            + "cu.gfs_customer_id as customer_id, cu.customer_name, cu.customer_status_code, "
            + "csl.display_short_desc as customer_status_desc, "
            + "to_char(cu.customer_status_date, 'YYYY-MM-DD') as customer_status_date, "
            + "cu.customer_store_nbr, " + "cu.non_profit_ind as non_profit_indicator, "
            + "cu.accept_orders_ind as accept_orders_indicator, " + "cu.credit_terms_code, "
            + "ctl.credit_terms_desc, " + "cut.sales_tax_exempt_id, " + "cu.new_sales_code, "
            + "nst.new_sales_desc as new_sales_desc, " + "cu.new_cust_ovrd_code as new_cust_override_code, "
            + "nco.new_cust_ovrd_desc as new_cust_override_desc, " + "cu.company_nbr, "
            + "to_char(cu.first_sale_date, 'YYYY-MM-DD') as first_sale_date, " + "cut.tax_exemption_code, "
            + "tet.tax_exemption_desc, " + "cu.cust_reln_mgr_id, "
            + "to_char(cu.customer_add_date,'YYYY-MM-DD') as customer_added_date, "
            + "coalesce(cu.credit_limit_dollar_amt,0) as credit_limit_dollar_amt, "
            + "cu.business_unit_code as customer_division_id, "
            + "cu.source_create_system_code as source_system_code, "
            + "sscode.code_desc as source_system_desc, " + "cu.primary_ship_dc_nbr, "
            + "cu.credit_risk_code, " + "crl.credit_risk_desc, " + "seg.customer_segment_code, "
            + "segdesc.code_desc as customer_segment_desc " + "from cust_admin.gfs_cust_unit cu "
            + "left outer join cust_admin.gfs_cust_unit_tax cut "
            + "   on cut.gfs_customer_id = cu.gfs_customer_id "
            + "left outer join cust_admin.tax_exemption_type_lang tet "
            + "   on tet.tax_exemption_code = cut.tax_exemption_code "
            + "left outer join cust_admin.customer_status_lang csl "
            + "   on csl.customer_status_code = cu.customer_status_code and csl.language_type_code = 'en' "
            + "left outer join cust_admin.credit_terms_lang ctl "
            + "   on ctl.credit_terms_code = cu.credit_terms_code and ctl.language_type_code = 'en' "
            + "left outer join cust_admin.new_cust_ovrd_type_lang nco "
            + "   on nco.new_cust_ovrd_code = cu.new_cust_ovrd_code and nco.language_type_code = 'en' "
            + "left outer join cust_admin.new_sales_type_lang nst "
            + "   on nst.new_sales_code = cu.new_sales_code and nst.language_type_code = 'en' "
            + "left outer join cust_admin.cust_codes_language_desc sscode "
            + "   on cast(sscode.code_value as smallint) = cu.source_create_system_code "
            + "     and sscode.code_table_ref_id = 33 and sscode.language_type_code = 'en' "
            + "left outer join cust_admin.credit_risk_lang crl "
            + "   on crl.credit_risk_code = cu.credit_risk_code and crl.language_type_code = 'en' "
            + "left outer join cust_admin.gfs_customer_segment_asgn seg "
            + "   on seg.gfs_customer_id = cu.gfs_customer_id " + "     and seg.gfs_customer_type_code = '0' "
            + "left outer join cust_admin.cust_codes_language_desc segdesc "
            + "   on cast(segdesc.code_value as smallint) = seg.customer_segment_code "
            + "     and segdesc.code_table_ref_id = 1 " + "     and segdesc.language_type_code = 'en'";

    /**
     * Options supported by the pipeline.
     * <p>
     * Inherits standard configuration options.
     * </p>
     */
    public interface MyOptions extends DataflowPipelineOptions {

        // @Description("Standardized naming convention value; for ex, just name
        // of topic ||
        // subscription")
        // @Default.String("INVOICE_ODS_ADMIN.INVOICE_LINE")
        // //@Required
        // ValueProvider<String> getStandardizedName();
        // void setStandardizedName(ValueProvider<String> value);
        //
        // @Description("The Cloud Pub/Sub subscription to read from. Just
        // name!")
        // @Default.String("INVOICE_ODS_ADMIN.INVOICE_LINE")
        // //@Hidden
        // ValueProvider<String> getPubSubSubscription();
        // void setPubSubSubscription(ValueProvider<String> value);

        @Description("The directory to output files to. Must end with a slash.  Can use YYYY, MM, DD, HH, mm for replacements")
        // @Required
        @Default.String("gs://gcp-gfs-data-pipeline/output/")
        ValueProvider<String> getOutputDirectory();

        void setOutputDirectory(ValueProvider<String> value);

        // @Description("The filename prefix of the files to write to.")
        // @Default.String("output")
        // //@Required
        // ValueProvider<String> getOutputFilenamePrefix();
        // void setOutputFilenamePrefix(ValueProvider<String> value);
        //
        // @Description("The suffix of the files to write. Defaults to .json")
        // @Default.String(".json")
        // ValueProvider<String> getOutputFilenameSuffix();
        // void setOutputFilenameSuffix(ValueProvider<String> value);
        //
        // @Description("The shard template of the output file. Specified as
        // repeating sequences "
        // + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced
        // with the "
        // + "shard number, or number of shards respectively")
        // @Default.String("W-P-SS-of-NN")
        // ValueProvider<String> getOutputShardTemplate();
        // void setOutputShardTemplate(ValueProvider<String> value);
        //
        // @Description("The maximum number of output shards produced when
        // writing.")
        // @Default.Integer(1)
        // Integer getNumShards();
        // void setNumShards(Integer value);
        //
        // @Description("The window duration in which data will be written.
        // Defaults to 2. ")
        // @Default.Integer(2)
        // Integer getWindowDurationSec();
        // void setWindowDurationSec(Integer value);
    }

}
