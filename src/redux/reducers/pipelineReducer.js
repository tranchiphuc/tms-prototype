const initialState = {
  flinkJobs: [],
  kafkaTopics: [],
  clickhouseSinks: [],
  loading: false,
};

export default function pipelineReducer(state = initialState, action) {
  switch (action.type) {
    default:
      return state;
  }
}
