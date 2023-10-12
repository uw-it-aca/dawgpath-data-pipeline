// hello-world.vue

<template>
  <p>{{ helloStore.message }}</p>
</template>

<script>
import { useHelloStore } from "@/stores/hello";

export default {
  name: "HelloWorld",
  data() {
    return {
      helloStore: useHelloStore(),
    };
  },
  methods: {},
};
</script>
