// home.vue

<template>
  <layout :page-title="pageTitle">
    <!-- page content -->
    <template #title>
      {{ pageTitle }}
    </template>
  </layout>
</template>

<script>
import Layout from "@/layout.vue";

export default {
  name: "PagesHome",
  components: {
    layout: Layout,
  },
  data() {
    return {
      pageTitle: "Dawgpath Pipeline Admin",
    };
  },
  methods: {},
};
</script>
