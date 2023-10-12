// about.vue

<template>
  <layout :page-title="pageTitle">
    <!-- page content -->
    <template #title>
      {{ pageTitle }}
    </template>

    <template #description>
      <p class="p-0 col-md-8 lead">
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas eget
        pulvinar est. Sed gravida neque justo, pellentesque finibus velit
        scelerisque eu. Curabitur et dolor luctus, convallis magna at, mollis
        purus. Nunc tincidunt risus quis nisl bibendum volutpat. Praesent ut
        tincidunt ex.
      </p>
    </template>

    <template #content>
      <div class="row">
        <div class="col-md-6">
          <h2>Layout</h2>
          <p>
            Start with the <code>/layout.vue</code> component to start
            customizing your application boilerplate (i.e. header and footer).
          </p>
        </div>
        <div class="col-md-6">
          <h2>Pages</h2>
          <p>
            Page components are located in the <code>/pages</code> directory and
            are mapped to router configuration located in
            <code>/router/index.js</code>.
          </p>
        </div>
      </div>

      <hr class="mb-5 w-25 d-inline-block" />

      <div class="row">
        <div class="col-md-12 pb-5">
          <h2>Components</h2>
          <p>
            Single-file components in Vue make up the building blocks of all
            applications. These components are located in the
            <code>/components</code>
            directory. The following <code>hello-world.vue</code> file is an
            example of a very simple component.
          </p>
          <hello-world />
        </div>
        <div class="col-md-6">fasdasdf</div>
        <div class="col-md-6">
          <h3 class="h5 text-muted">AXDD Components</h3>
          <p>
            These are published and maintained components intended to be used
            across AXDD applications. The following
            <code>axdd-card</code> component is a standard content container.
          </p>

          <axdd-card>
            <template #body>
              <p class="font-weight-lighter">
                Lasdorem ipsum dolor sit amet, consectetur adipiscing elit.
                Maecenas eget pulvinar est. Sed gravida neque justo,
                pellentesque finibus velit scelerisque eu.
              </p>
            </template>
          </axdd-card>

          <h3 class="h5 text-muted">Vue3 MQ (Bootstrap5 preset)</h3>
          <div>{{ mq }}</div>
        </div>
      </div>
    </template>
  </layout>
</template>

<script>
import Layout from "@/layout.vue";
import HelloWorld from "@/components/hello-world.vue";

export default {
  name: "PagesCustomize",
  inject: ["mq"],
  components: {
    layout: Layout,
    HelloWorld,
  },
  data() {
    return {
      pageTitle: "Customizing your app",
    };
  },
  methods: {},
};
</script>
