SELECT 
    st.registered_class AS class,
    s.subject_name AS subject,
    er.exam_event,
    ROUND(AVG(er.exam_score), 2) AS average_score,
    MAX(er.exam_score) AS max_score,
    MIN(er.exam_score) AS min_score,
    ROUND(STDDEV(er.exam_score), 2) AS standard_deviation
FROM
    exam_result er
JOIN 
    subject s ON er.subject_id = s.subject_id
JOIN 
    student st ON er.student_id = st.student_id 
GROUP BY
    st.registered_class, 
    s.subject_name,
    er.exam_event
ORDER BY
    st.registered_class, s.subject_name;