import { describe, it, expect } from "vitest";
import { mount } from "@vue/test-utils";
import { createPinia } from "pinia";

import HelloWorld from "@/components/hello-world.vue";

describe("HelloWorld", () => {
  it("displays hello world from pinia", () => {
    const wrapper = mount(HelloWorld, {
      global: {
        plugins: [createPinia()],
      },
    });
    // Assert the rendered text of the component
    expect(wrapper.text()).toContain("Hello world, from Pinia");
  });
});
